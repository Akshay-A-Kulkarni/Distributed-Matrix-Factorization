package matrix_factorization

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.Not

import scala.collection.mutable
import scala.util.Random
import org.apache.spark.sql.SparkSession


object factorization {

  val n_factors = 10;
  val n_iterations = 10

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val conf = new SparkConf().setAppName("Factorize Matrix").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).appName("Factorize Matrix").getOrCreate()
    import spark.implicits._

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(args(0)), true)
    } catch {
      case _: Throwable => {}
    }
    // ================

    // Define partitioner
    val partitioner = new HashPartitioner(1)


        }
}
