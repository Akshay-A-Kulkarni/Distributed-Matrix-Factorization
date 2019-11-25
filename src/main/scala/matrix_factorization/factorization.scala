package matrix_factorization

import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object factorization {

  val nFactors = 10
  val convergenceIterations = 10
  val seedVal = 123

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

    val mySpark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import mySpark.implicits._

    val sc = mySpark.sparkContext

    val inputRDD = sc.textFile("input/small.txt")
      .map { line => {
        val list = line.split(",")
        (list(0).toInt, (list(1).toInt, list(2).toInt))
        }
      }

    val rand = scala.util.Random

    val sortedUsers = sortByRelativeIndex("user", inputRDD)
    val sortedItems = sortByRelativeIndex("item", inputRDD)

    // Link information for users and items
    val user_blocks = getBlocks("user", inputRDD, sortedUsers, sortedItems)
    val item_blocks = getBlocks("item", inputRDD, sortedUsers, sortedItems)

    // initialising random Factor matrices
    val P = user_blocks.mapPartitionsWithIndex { (idx, iter) =>
      val rand = new scala.util.Random(idx + seedVal)
      iter.map(x => (x._1, Seq.fill(nFactors)(rand.nextInt(5))))
    }

    val Q = item_blocks.mapPartitionsWithIndex { (idx, iter) =>
      val rand = new scala.util.Random(idx + seedVal)
      iter.map(x => (x._1, Seq.fill(nFactors)(rand.nextInt(5))))
    }

    user_blocks.foreach(println)
    item_blocks.foreach(println)

    //    P.foreach(println)
    //    Q.foreach(println)
  }

  def sortByRelativeIndex(bType: String, input: RDD[(Int, (Int, Int))]): Array[(Int, Long)] = {
    bType match {
      case "user" => {
        return input
          .map(line => line._1)
          .distinct()
          .sortBy(idx => idx, true, 1)
          .zipWithIndex()
          .collect()
      }
      case "item" => {
        return input
          .map(line => line._2._1)
          .distinct()
          .sortBy(idx => idx, true, 1)
          .zipWithIndex()
          .collect()
      }
    }
  }

  def getRelativeIndex(indexToFind: Int, relativeIndexList: Array[(Int, Long)]): Long = {
    return relativeIndexList
      .filter(data => data._1 == indexToFind)
      .map(data => data._2)
      .head
  }

  def getBlocks(bType: String, R: RDD[(Int, (Int, Int))], sortedUsers: Array[(Int, Long)], sortedItems: Array[(Int, Long)]): RDD[(Long, Iterable[Long])] = {
    /*
    @params
    bType : Str -> block type  ("user"/"item")
    R     : RDD -> initial ratings RDD

    The function takes in the input RDD to assign contiguous indices to users and items and computes the
    user->item and item->user links.

    (note : indexing functionality incomplete)
    */
    bType match {
      case "user" => {
        val userBlocks = R.map { case (u, (i, v)) => (getRelativeIndex(u, sortedUsers), getRelativeIndex(i, sortedItems)) }.groupByKey()
        return userBlocks
      }
      case "item" => {
        val itemBlocks = R.map { case (u, (i, v)) => (getRelativeIndex(i, sortedItems), getRelativeIndex(u, sortedUsers)) }.groupByKey()
        return itemBlocks
      }
    }
  }

}
