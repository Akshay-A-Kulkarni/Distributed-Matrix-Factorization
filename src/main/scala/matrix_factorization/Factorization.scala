package matrix_factorization


import breeze.linalg._
import breeze.linalg.{DenseMatrix, DenseVector, pinv}
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object Factorization {

  val nFactors: Int = 3
  val seedVal: Int = 123
  val minRating: Int = 1
  val maxRating: Int = 5
  val convergenceIterations: Int = 3
  val lambda: Double = 0.01
  val maxFilter = 50000

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    // Delete output directory, only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(args(0)), true)
    } catch {
      case _: Throwable => {}
    }
    // ================

    val conf = new SparkConf()
      .setAppName("MatrixFactorization")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val sc = spark.sparkContext

    val partitioner = new HashPartitioner(5)

    val residual = sc.doubleAccumulator
    val pu_norm = sc.doubleAccumulator
    val qi_norm = sc.doubleAccumulator

    val inputRDD = sc.textFile("input/small.txt")
      .map { line => {
        val list = line.split(",")
        (list(0).toInt, (list(1).toInt, list(2).toInt))}
      }
      .filter{
          case (userId,(movieId, rating)) =>
              userId <= maxFilter
        }
      .partitionBy(partitioner)

    val sortedUsers = sortByRelativeIndex("user", inputRDD)
    val sortedItems = sortByRelativeIndex("item", inputRDD)

    // Link information for users and items
    val user_blocks = getBlocks("user", inputRDD, sortedUsers, sortedItems)
    val item_blocks = getBlocks("item", inputRDD, sortedUsers, sortedItems)

    // Creating two Ratings Matrices partitioned by user and item respectively
    val R_u = inputRDD.map { case (u, (i, v)) => (getRelativeIndex(u, sortedUsers), (getRelativeIndex(i, sortedItems), v)) }
      .cache()

    val R_i = R_u.map(i => (i._2._1, (i._1, i._2._2))).partitionBy(partitioner).cache()

//
//  val R_u = inputRDD.cache()
//  val R_i = R_u.partitionBy(partitioner).cache()


    // initialising random Factor matrices
//    val P = user_blocks.mapPartitionsWithIndex { (idx, row) =>
//      val rand = new scala.util.Random(idx + seedVal)
//      row.map(x => (x._1, DenseMatrix.fill(1, nFactors)(minRating + rand.nextDouble() * (maxRating - minRating) + 1)))
////      row.map(x => (x._1, Seq.fill(nFactors)(minRating + rand.nextDouble() * (maxRating - minRating) + 1)))
//    }.collect()
//
//    val Q = item_blocks.mapPartitionsWithIndex { (idx, row) =>
//      val rand = new scala.util.Random(idx + seedVal)
//      row.map(x => (x._1, DenseMatrix.fill(1, nFactors)(minRating + rand.nextDouble() * (maxRating - minRating) + 1)))
//      // row.map(x => (x._1, Seq.fill(nFactors)(minRating + rand.nextDouble() * (maxRating - minRating) + 1)))
//    }.collect()

    val rand = new scala.util.Random(seedVal)
    var P = DenseMatrix.fill(nFactors, sortedUsers.length)(minRating + rand.nextDouble() * (maxRating - minRating) + 1)
    var Q = DenseMatrix.fill(nFactors, sortedItems.length)(minRating + rand.nextDouble() * (maxRating - minRating) + 1)

    var q_bcast = sc.broadcast(Q)
    var p_bcast = sc.broadcast(P)

    var iter = 0
    var totalIter = 3
    var totalCost = Double.MaxValue

    var tolerance = 0.005
    while(totalCost >= tolerance) {

      // #### Step to calculate New P ####

      // calculates gradient for new P in RDD form
      var newP = computeGradient(R_u, q_bcast, lambda)
        .sortByKey()
        .map(data => data._2)
        .collect()

      // converts newP to a new dense matrix P
      P = DenseMatrix(newP.map(_.toArray):_*).t

      // Rebroadcast P
      p_bcast.unpersist()
      p_bcast = sc.broadcast(P)

      // #### Step to calculate New Q ####

      // calculates gradient for new Q in RDD form
      var newQ = computeGradient(R_i, p_bcast, lambda)
        .sortByKey()
        .map(data => data._2)
        .collect()

      // converts newQ to a new dense matrix Q
      Q = DenseMatrix(newQ.map(_.toArray):_*).t

      // Rebroadcast Q
      q_bcast.unpersist()
      q_bcast = sc.broadcast(Q)

      // #### Step to compute cost ####

      R_u.foreach{ case (userId, (movieId, r_ij)) =>
          val q_i = Q(::, movieId.toInt)
          val p_u = P(::, userId.toInt)

          residual.add( math.pow(r_ij - (p_u.t * q_i), 2))
      }


      totalCost = residual.sum
      println("Iteration(" + iter + ") Cost: " + totalCost)
      residual.reset()

      iter += 1
    }
  }


  def computeGradient(R: RDD[(Long, (Long, Int))], constantLatentMatrix: Broadcast[DenseMatrix[Double]], lambda: Double): RDD[(Long, DenseMatrix[Double])] = {
    /*
    @params
    Q: DenseMatrix[Double]    : Broadcasted dense latent factor matrix
    R: RDD[(Long,(Long,Int))] : Input RDD of the dense representation of the rating matrix

    Note : Possible addition of a Lambda param.

    Computes the gradient step and updates for each latent factor matrix.
    */

    var temp = R.groupByKey()
    val optimizedMatrix = temp.mapValues(values => values.unzip)
    optimizedMatrix.mapValues { case (colList, rateList) =>
      var col = pinv(computeTransposeProductSum(colList, constantLatentMatrix.value) + lambda *:* DenseMatrix.eye[Double](constantLatentMatrix.value.rows)) * computeRatingProduct(colList, rateList, constantLatentMatrix.value)

      col
    }
  }

  def computeTransposeProductSum(columns: Iterable[Long], M: DenseMatrix[Double]): DenseMatrix[Double] = {
    /*
    @params
    columns: Iterable[Long] : list of indices associated with given user/item.
    M: DenseMatrix[Double]    : Broadcasted dense latent factor matrix

    */
    var LatentFactorSum = DenseMatrix.zeros[Double](M.rows, M.rows)
    for (i <- columns.toList) {
      val C = M(::, i.toInt)
      val ColProd = C * C.t
      LatentFactorSum :+= ColProd
    }
    return LatentFactorSum
  }

  def computeRatingProduct(columns: Iterable[Long], ratings: Iterable[Int], M: DenseMatrix[Double]): DenseMatrix[Double] = {
    /*
    @params
    columns: Iterable[Long] : list of indices associated with given user/item.
    ratings: Iterable[Int]
    M: DenseMatrix[Double]    : Broadcasted dense latent factor matrix
*/

    var RatingProdFactorSum = DenseVector.zeros[Double](M.rows)
    val TList = ratings.toList.zip(columns.toList)
    for (i <- TList) {
      val r_ui = i._1.toDouble
      val C = M(::, i._2.toInt)
      val result = r_ui *:* C
      RatingProdFactorSum :+= result
    }
    return RatingProdFactorSum.toDenseMatrix.t
  }

  def sortByRelativeIndex(bType: String, input: RDD[(Int, (Int, Int))]): Array[(Int, Long)] = {
    /*
    @params
    bType : Str -> block type  ("user"/"item")
    R     : RDD -> initial ratings RDD

    The function takes in the input RDD to assign each unique user/item ID to a relative index
    and sorts list in ascending order.

    */
    bType match {
      case "user" => {
        return input
          .map(line => line._1)
          .distinct()
          .sortBy(idx => idx, ascending = true, 1)
          .zipWithIndex()
          .collect()
      }
      case "item" => {
        return input
          .map(line => line._2._1)
          .distinct()
          .sortBy(idx => idx, ascending = true, 1)
          .zipWithIndex()
          .collect()
      }
    }
  }


  def getRelativeIndex(valueToFind: Int, relativeIndexList: Array[(Int, Long)]): Long = {
    /*
    @params
    valueToFind       : Int -> user/item value to look up
    relativeIndexList : Array -> (value, index) lookup array

    This function takes input a value and a lookup table of (value, index) and returns the index for a given value).
    Note: Each value is an unique identifier such that there will be no duplicates in lookup.
    Each value->index relationship is 1-to-1

    */
    return relativeIndexList
      .filter(data => data._1 == valueToFind)
      .map(data => data._2)
      .head
  }

  def getBlocks(bType: String, R: RDD[(Int, (Int, Int))], sortedUsers: Array[(Int, Long)],
                sortedItems: Array[(Int, Long)]): RDD[(Long, Iterable[Long])] = {
    /*
    @params
    bType : Str -> block type  ("user"/"item")
    R     : RDD -> initial ratings RDD

    The function takes in the input RDD to assign contiguous indices to users and items and computes the
    user->item and item->user links.

    */
    bType match {
      case "user" => {
        val userBlocks = R.map { case (u, (i, v)) => (getRelativeIndex(u, sortedUsers), getRelativeIndex(i, sortedItems))
        }.groupByKey()
        return userBlocks
      }
      case "item" => {
        val itemBlocks = R.map { case (u, (i, v)) => (getRelativeIndex(i, sortedItems), getRelativeIndex(u, sortedUsers))
        }.groupByKey()
        return itemBlocks
      }
    }
  }
}
