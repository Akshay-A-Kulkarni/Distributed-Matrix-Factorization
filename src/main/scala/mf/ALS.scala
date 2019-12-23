package mf

import breeze.linalg._
import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALS {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length < 4) {
      logger.error("Usage:\nmf.ALS <INPUT_PATH> <MAX_FILTER> <NUM_ITER> <LAMBDA>")
      System.exit(1)}
    val hadoopConf = new org.apache.hadoop.conf.Configuration
// Delete output directory, only to ease local development; will not work on AWS.
//  val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//        try {
//          hdfs.delete(new org.apache.hadoop.fs.Path(args(0)), true)
//        } catch {
//          case _: Throwable => {}
//        }
// ================
    val nFactors: Int = 50
    val seedVal: Int = 123
    val minRating: Int = 1
    val maxRating: Int = 5
    val convergenceIterations: Int = args(2).toInt
    val lambda: Double = args(3).toDouble

    val conf = new SparkConf()
      .setAppName("ALSMatrixFactorization")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext
    val maxFilter = args(1).toInt

    val inputRDD = sc.textFile(args(0))
      .map { line => {
        val list = line.split(",")
        (list(0).toInt, (list(1).toInt, list(2).toInt))}
      }
     .filter{
        case (userId,(movieId, rating)) =>
          userId <= maxFilter
      }
    val sortedUsers = sortByRelativeIndex("user", inputRDD)
    val sortedItems = sortByRelativeIndex("item", inputRDD)

    // Link information for users and items
    val user_blocks = getBlocks("user", inputRDD, sortedUsers, sortedItems)
    val item_blocks = getBlocks("item", inputRDD, sortedUsers, sortedItems)

    // Creating two Ratings Matrices partitioned by user and item respectively
    val R_u = inputRDD.map { case (u, (i, v)) => (getRelativeIndex(u, sortedUsers), (getRelativeIndex(i, sortedItems), v)) }
      .cache()
    val R_i = R_u.map(i => (i._2._1, (i._1, i._2._2))).cache()

    val rand = new scala.util.Random(seedVal)
    val rand1 = new scala.util.Random(seedVal+1)

    var P = DenseMatrix.fill(nFactors, sortedUsers.length)(minRating + rand.nextDouble() * (maxRating - minRating) + 1)
    var Q = DenseMatrix.fill(nFactors, sortedItems.length)(minRating + rand.nextDouble() * (maxRating - minRating) + 1)

    var q_bdcast = sc.broadcast(Q)
    var p_bdcast = sc.broadcast(P)

    val iterations  = sc.longAccumulator
    var totalCost : Double = 0.0
    val tolerance = 0.05
    val prevCost = sc.doubleAccumulator
    val costDiff = sc.doubleAccumulator
    costDiff.add(Double.MaxValue)
    var costDiffDelta = Double.MaxValue
    val residual = sc.doubleAccumulator
    var costHistory = sc.broadcast(Seq[Double]())

    while(costDiffDelta >= tolerance && iterations.value < convergenceIterations ) {
      // Step to calculate New P
      // Calculates gradient for new P in RDD form
      val newP = R_u.groupByKey()
        .mapValues(row => computeGradient(row,q_bdcast,lambda))
        .sortByKey()
        .map(data => data._2)
        .collect()

      // converts newP to a new dense matrix P
      var P = DenseMatrix(newP.map(_.toArray):_*).t

      // Rebroadcast P
      p_bdcast.destroy()
      p_bdcast = sc.broadcast(P)

      // calculates gradient for new Q in RDD form
      val newQ = R_i.groupByKey()
        .mapValues(row => computeGradient(row,p_bdcast,lambda))
        .sortByKey()
        .map(data => data._2)
        .collect()

      // converts newQ to a new dense matrix Q
      var Q = DenseMatrix(newQ.map(_.toArray):_*).t

      // Rebroadcast Q
      q_bdcast.destroy()
      q_bdcast = sc.broadcast(Q)

      // #### Step to compute cost ####
      R_u.foreach{ case (userId, (movieId, r_ij)) =>
        val q_i = Q(::, movieId.toInt)
        val p_u = P(::, userId.toInt)
        residual.add(math.pow(r_ij - (p_u.t * q_i), 2))}
      val pu_norm = sum(sum(P *:* P, Axis._0))
      val qi_norm = sum(sum(Q *:* Q, Axis._0))
      totalCost = residual.sum + (lambda * (pu_norm + qi_norm))

      // Step to compute the cost difference (costDiff) and the change in costDiff, which is compared to the tolerance
      val prevcostDiff = costDiff.value
      costDiff.reset()
      costDiff.add(math.abs(totalCost - prevCost.value))
      costDiffDelta = math.abs(costDiff.value - prevcostDiff)/costDiff.value

      logger.info("Iteration(" + (iterations.value + 1) + ") Cost: " + totalCost + " Delta: " + costDiff.value + " DeltaDelta: " + costDiffDelta)
      println("Iteration(" + (iterations.value + 1) + ") Cost: " + totalCost + " Delta: " + costDiff.value + " DeltaDelta: " + costDiffDelta)

      residual.reset()
      prevCost.reset()
      prevCost.add(totalCost)
      residual.reset()

      costHistory =sc.broadcast(costHistory.value :+ totalCost)
      iterations.add(1)
    }
    logger.info("Cost History")
    logger.info(costHistory.value)
  }

  def computeGradient(R: Iterable[(Long, Int)], constantLatentMatrix: Broadcast[DenseMatrix[Double]], lambda: Double)
                                                                                              :  DenseMatrix[Double] = {
    /*
    @params
    Q: DenseMatrix[Double]       : Broadcasted dense latent factor matrix
    R: (Long,Iterable(Long,Int)) : a row of grouped Input RDD of the dense representation of the rating matrix

    Note : Possible addition of a Lambda param.

    Computes the gradient step and updates for each latent factor matrix.
    */
    val listsTuple = R.unzip
    val optimizedMatrix =
      inv(computeTransposeProductSum(listsTuple._1, constantLatentMatrix.value) + lambda *:* DenseMatrix.eye[Double]            (constantLatentMatrix.value.rows)) * computeRatingProduct(listsTuple._1, listsTuple._2, constantLatentMatrix.value)
    return optimizedMatrix
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

  def computeRatingProduct(columns: Iterable[Long], ratings: Iterable[Int], M: DenseMatrix[Double])
                                                                                            : DenseMatrix[Double] = {
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

    This function takes in a value and a lookup table of (value, index) and returns the index for a given value).
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
