/*
 * Distributed line count: Assumes a parallel/networked filesystem in this
 * version.
 */

package edu.luc.cs

import java.io._
import java.net._
import java.nio.file._
import scala.util.Try
import org.apache.spark._
import cs.luc.edu.performance._
import cs.luc.edu.fileutils._
import breeze.linalg._

object BreezeSparkBenchmark {

  val DEFAULT_DIMENSION = 2048
  val DEFAULT_NODES = 4
  val DEFAULT_PARTITIONS = 48
  val DEFAULT_WORKLOAD = DEFAULT_NODES * DEFAULT_PARTITIONS
  val DEFAULT_OUTPUT_DIR = "./results"
  val DEFAULT_CACHE_POLICY = false

  // This is the Scala way of doing a "struct". This allows us to change what is computed 
  // without having to change anything but countLinesInFile()

  case class Data(array: Array[DenseMatrix[Double]], time: Time, space: Space, hostname: String)

  case class Config(dim: Option[Int] = None, nodes: Option[Int] = None, partitions: Option[Int] = None, workload: Option[Int] = None, outputDir: Option[String] = None)

  // This function is evaluated in parallel via the RDD

  def getDoubleMatrix(dim: Int) = DenseMatrix.fill[Double](dim, dim) { math.random }

  def allocate3D(slice: Int, dim: Int): Data = {
    val (timeAllocate3D, memAllocate3D, array3D) = performance {
      Array.fill(dim)(getDoubleMatrix(dim))
    }
    Data(array3D, timeAllocate3D, memAllocate3D, InetAddress.getLocalHost.getHostName)
  }

  def do3D(a: Data): Double = {
    a.array.map { matrix => trace(matrix) }.sum
  }

  def parseCommandLine(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("breeze-spark-demo", "1.0")
      opt[Int]('d', "dim") action { (x, c) =>
        c.copy(dim = Some(x))
      } text (s"dim is the matrix dimension (default = $DEFAULT_NODES)")

      opt[Int]('p', "partitions") action { (x, c) =>
        c.copy(partitions = Some(x))
      } text (s"partitions is RDD partition size (default = $DEFAULT_PARTITIONS)")

      opt[Int]('n', "nodes") action { (x, c) =>
        c.copy(nodes = Some(x))
      } text (s"nodes is the number of cluster nodes being used (default $DEFAULT_NODES)")

      opt[Int]('w', "workload") action { (x, c) =>
        c.copy(workload = Some(x))
      } text (s"workload is number of matrix operations to do (default $DEFAULT_NODES * $DEFAULT_PARTITIONS)")

      opt[String]('o', "outputdir") action { (x, c) =>
        c.copy(outputDir = Some(x))
      } text (s"outputDir is where to write the benchmark results (default $DEFAULT_OUTPUT_DIR)")

      help("help") text ("prints this usage text")

    }
    // parser.parse returns Option[C]
    parser.parse(args, Config())
  }

  def main(args: Array[String]) = go(args)

  def go(args: Array[String]) = {
    val conf = new SparkConf().setAppName("LinearAlgebra File I/O")
    val spark = new SparkContext(conf)
    val appConfig = parseCommandLine(args) getOrElse (Config())
    val dim = appConfig.dim.getOrElse(DEFAULT_DIMENSION)
    val partitions = appConfig.partitions.getOrElse(DEFAULT_PARTITIONS)
    val nodes = appConfig.nodes.getOrElse(DEFAULT_NODES)
    val workload = appConfig.workload.getOrElse(nodes * partitions)
    val outputDir = appConfig.outputDir.getOrElse(DEFAULT_OUTPUT_DIR)

    // create RDD from generated file listing

    val (rddElapsedTime, rddMemUsed, rdd) = performance {
      spark.parallelize(1 to workload, partitions).map {
        slice => allocate3D(slice, dim)
      }
    }

    val (t3dElapsedTime, t3dMemUsed, trace3dSum) = performance {
      rdd map { a3d => do3D(a3d) } reduce (_ + _)
    }

    // This is to write information about cluster usage.
    val nodeFileName = s"${outputDir}/nodes-dim=${dim}-nodes=${nodes}-partitions=${partitions}-workload=${workload}.txt"
    val nodeFileWriter = new PrintWriter(new File(nodeFileName))
    nodeFileWriter.println("Node Usage (on cluster)")
    val pairs = rdd.map(lc => (lc.hostname, 1))
    val counts = pairs.reduceByKey((a, b) => a + b)
    val nodesUsed = counts.collect() foreach nodeFileWriter.println
    nodeFileWriter.close

    // Write experimental results
    // The file will be named uniquely by dimensions/nodes/partitions/workload
    // TODO: I will move all non-performance output to the log file above.

    var results = Map(
      "dim" -> s"${dim}",
      "partitions" -> s"${partitions}",
      "nodes" -> s"${nodes}",
      "workload" -> s"${workload}",
      "rddElapsedTime" -> s"rddElapsedTime=${rddElapsedTime}",
      "t3dElapsedTime" -> s"${t3dElapsedTime}"
    )
    val resultsFileName = s"${outputDir}/results-dim=${dim}-nodes=${nodes}-partitions=${partitions}-workload=${workload}.txt"
    val writer = new PrintWriter(new File(resultsFileName))
    val asKeyValText = results map { case (name, value) => s"${name}=${value}" }
    asKeyValText foreach { text => writer.println(text) }
    writer.close()
    spark.stop()
  }
}
