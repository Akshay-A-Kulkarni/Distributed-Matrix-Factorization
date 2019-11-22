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

  // Initialize k, number of iterations, and number of total vertices in synthetic graph

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

    // Create synthetic graph
    val synthetic_graph = sc.parallelize(generateGraph(k)).partitionBy(partitioner)

    // Create an adjacency list RDD with (key, adj_list val) pairs
    val links  = synthetic_graph.groupByKey()
    var page_ranks = links.mapValues(node => (1.0 / (vertices)))

    // Compute sink node rank for dangling nodes and set its page rank score to to 0.
    // Add the sink node to the page ranks RDD
    val sink_node_rank = sc.parallelize(List((0, 0.0))).partitionBy(partitioner)
    page_ranks = page_ranks.union(sink_node_rank)

    //Iterate through the graph
    for (itr_count <- 1 to iterations){

        // Compute the incoming contributions for all nodes that receive in-links and aggregate the PR value per node
        val inc_contribs = links.join(page_ranks)
          .flatMap{case(pageID, (links, rank)) => links.map(page => (page, rank/links.size))}
          .reduceByKey(_+_)

        // Compute and collect the sink nodes mass from all incoming contributions into sink node
        val sink_node_mass = inc_contribs.lookup(0)(0)

        // Distribute the mass evenly among all nodes
        val mass_distribution = sink_node_mass / (vertices)

        // Compute final contributions, filtering out the sink node
        val full_contributions = page_ranks
          .leftOuterJoin(inc_contribs)
          .filter(_._1 != 0)
          .mapValues{case(x,y) => if (y.isDefined) y.get else 0.0 }

        // Update the page rank values for current iteration
        page_ranks = full_contributions.mapValues(v => 0.15/(vertices) + 0.85*(v + mass_distribution)).persist()
    }

    page_ranks.sortBy(_._1, true).saveAsTextFile(args(0))
    logger.info(page_ranks.toDebugString)

        }
}