package matrix_factorization

import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object factorization {

  val n_factors = 10;
  val n_iterations = 10

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

//     val spark = SparkSession
//       .builder()
//       .config(conf)
//       .getOrCreate()

    val mySpark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()


    // For implicit conversions like converting RDDs to DataFrames
    import mySpark.implicits._

    val sc = mySpark.sparkContext


    var RDD_dataset = sc.textFile("input/small.txt")
      .map(data => data.split(","))
      .map(data => (data(0).toInt, data(1).toInt, data(2).toInt))

    RDD_dataset.foreach(println)
  }
}
