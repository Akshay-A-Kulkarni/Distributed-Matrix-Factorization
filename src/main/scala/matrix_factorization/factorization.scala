package matrix_factorization

import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object factorization {

  val n_factors = 10;
  val convergence_iterations = 10
  val SeedVal = 123

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
                          .map{ line => {
                            val list = line.split(",")
                            (list(0).toInt, (list(1).toInt, list(2).toInt))
                            }
                          }

    val rand = scala.util.Random

    // Link information for users and items
    val user_blocks = getBlocks("user",inputRDD)
    val item_blocks = getBlocks("item",inputRDD)

    // initialising random Factor matrices
    val P = user_blocks.mapPartitionsWithIndex { (idx, iter) =>
      val rand = new scala.util.Random(idx+SeedVal)
      iter.map(x => (x._1,Seq.fill(n_factors)(rand.nextInt(5))))
    }


    val Q = item_blocks.mapPartitionsWithIndex { (idx, iter) =>
      val rand = new scala.util.Random(idx+SeedVal)
      iter.map(x => (x._1,Seq.fill(n_factors)(rand.nextInt(5))))
    }

    P.foreach(println)
    Q.foreach(println)
  }

  def getBlocks(bType: String, R : RDD[(Int,(Int,Int))]): RDD[(Int,Iterable[Int])] = {
    /*
    @params
    bType : Str -> block type  ("user"/"item")
    R     : RDD -> initial ratings RDD

    The function takes in the input RDD to assign contiguous indices to users and items and computes the
    user->item and item->user links.

    (note : indexing functionality incomplete)
    */
    bType match {
                  case "user"  => {
                                    val userBlocks = R.map{ case (u,(i,v)) => (u,i) }.groupByKey()
                                    return userBlocks }
                  case "item"  => {
                                    val itemBlocks= R.map{ case (u,(i,v)) => (i,u) }.groupByKey()
                                    return itemBlocks }
    }
  }

  }
