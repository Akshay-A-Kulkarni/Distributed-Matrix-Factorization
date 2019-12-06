package matrix_factorization

import breeze.linalg.DenseMatrix
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object Factorization {

  val nFactors  : Int = 10
  val seedVal   : Int = 123
  val minRating : Int = 1
  val maxRating : Int = 5
  val convergenceIterations : Int = 10

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

    val inputRDD = sc.textFile("input/small.txt")
      .map { line => {
        val list = line.split(",")
        (list(0).toInt, (list(1).toInt, list(2).toInt))
        }
      }.partitionBy(partitioner)

//    (getRelativeIndex(u, sortedUsers), getRelativeIndex(i, sortedItems)

    val sortedUsers = sortByRelativeIndex("user", inputRDD)
    val sortedItems = sortByRelativeIndex("item", inputRDD)

    // Link information for users and items
    val user_blocks = getBlocks("user", inputRDD, sortedUsers, sortedItems)
    val item_blocks = getBlocks("item", inputRDD, sortedUsers, sortedItems)

    // Creating two Ratings Matrices partitioned by user and item respectively
    val R_u = inputRDD.map{case (u, (i, v)) => (getRelativeIndex(u, sortedUsers), (getRelativeIndex(i, sortedItems),v))}
      .cache()

    val R_i = R_u.map(i => (i._2._1, (i._1, i._2._2))).partitionBy(partitioner).cache()


//    val R_u = inputRDD.cache()
//    val R_i = R_u.partitionBy(partitioner).cache()


    // initialising random Factor matrices
    val P = user_blocks.mapPartitionsWithIndex { (idx, row) =>
      val rand = new scala.util.Random(idx + seedVal)
      row.map(x => (x._1, Seq.fill(nFactors)(minRating + rand.nextInt((maxRating - minRating) + 1 ))))
    }.collect()

    val Q = item_blocks.mapPartitionsWithIndex { (idx, row) =>
      val rand = new scala.util.Random(idx + seedVal)
      row.map(x => (x._1, Seq.fill(nFactors)(minRating + rand.nextInt((maxRating - minRating) + 1 ))))
    }.collect()


    val U = sc.broadcast(P)
    val M = sc.broadcast(Q)

    // loop:

    var p_us = R_u
        .groupByKey()
      .foreach( row => getNewLatentColumn(row, U, M))

    // converts p_us to a matrix and rebroadcasts P

    var q_is = R_i
      .groupByKey()
      .foreach( column => getNewLatentColumn(column, U, M))

    // converts Q_Is to a matrix and rebroadcasts Q

    // compute cost

    // check for minimziation of cost





//    R_i.mapPartitionsWithIndex( (index: Int, it: Iterator[Long]) =>
//      it.toList.map(x => if (index ==5) {println(x)}).iterator).collect
//    Q.foreach(println)
  }


  def getNewLatentColumn(input: (Long, Iterable[(Long, Int)]), P: Broadcast[Array[(Long, Seq[Int])]], Q: Broadcast[Array[(Long, Seq[Int])]]): DenseMatrix[Double] = {

    var key = input._1

    var data = input._2
    var nonEmptyColumnIndicies = data.map( d => d._1)

    println(key, nonEmptyColumnIndicies)

    var new_latent_column_for_key = DenseMatrix.rand[Double](nFactors, 1)
    return new_latent_column_for_key
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
        val userBlocks = R.map {case (u, (i, v)) => (getRelativeIndex(u, sortedUsers), getRelativeIndex(i, sortedItems))
                                  }.groupByKey()
        return userBlocks
      }
      case "item" => {
        val itemBlocks = R.map {case (u, (i, v)) => (getRelativeIndex(i, sortedItems), getRelativeIndex(u, sortedUsers))
                                  }.groupByKey()
        return itemBlocks
      }
    }
  }
}
