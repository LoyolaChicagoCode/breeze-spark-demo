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

  // This is the Scala way of doing a "struct". This allows us to change what is computed 
  // without having to change anything but countLinesInFile()

  case class Results(result: Double, time: Time, space: Space)

  case class Config(dim: Option[Int] = None, nodes: Option[Int] = None, slices: Option[Int] = None, workload: Option[Int] = None)

  // This function is evaluated in parallel via the RDD

  def getDoubleMatrix(dim: Int) = DenseMatrix.fill[Double](dim, dim) { math.random }
  def do3D(slice: Int, dim: Int): Results = {

    // this is a function (closure)

    val (t3d, m3d, sumTrace2d) = performance {
      val a = Array.fill(dim)(getDoubleMatrix(dim))
      a.par.map { matrix => trace(matrix) }.sum
    }

    Results(sumTrace2d, t3d, m3d)
  }

  def do2DOnly(slice: Int, dim: Int): Results = {
    val (time2d, mem2d, trace2d) = performance {
      val matrix = getDoubleMatrix(dim)
      trace(matrix)
    }
    Results(trace2d, time2d, mem2d)
  }

  def parseCommandLine(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("breeze-spark-demo", "1.0")
      opt[Int]('d', "dim") action { (x, c) =>
        c.copy(dim = Some(x))
      } text (s"dim is the matrix dimension (default = $DEFAULT_NODES)")

      opt[Int]('s', "slices") action { (x, c) =>
        c.copy(slices = Some(x))
      } text (s"slices is RDD partition size (default = $DEFAULT_PARTITIONS)")

      opt[Int]('n', "nodes") action { (x, c) =>
        c.copy(nodes = Some(x))
      } text (s"nodes is the number of cluster nodes being used (default $DEFAULT_NODES)")

      opt[Int]('w', "workload") action { (x, c) =>
        c.copy(workload = Some(x))
      } text (s"workload is number of matrix operations to do (default $DEFAULT_NODES * $DEFAULT_PARTITIONS)")

      help("help") text ("prints this usage text")

    }
    // parser.parse returns Option[C]
    parser.parse(args, Config())
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LinearAlgebra File I/O")
    val spark = new SparkContext(conf)
    val appConfig = parseCommandLine(args).getOrElse(Config())
    val dim = appConfig.dim.getOrElse(DEFAULT_DIMENSION)
    val slices = appConfig.slices.getOrElse(DEFAULT_PARTITIONS)
    val nodes = appConfig.nodes.getOrElse(DEFAULT_NODES)

    // create RDD from generated file listing

    val (rddElapsedTime, rddMemUsed, rdd) = performance {
      spark.parallelize(1 to slices, slices).map {
        slice => do3D(slice, dim)
      }
    }

    val (t3dElapsedTime, t3dMemUsed, trace3dSum) = performance {
      rdd map { _.result } reduce (_ + _)
    }

    val (rdd2ElapsedTime, rdd2MemUsed, rdd2) = performance {
      spark.parallelize(1 to slices, slices).map {
        slice => do2DOnly(slice, dim)
      }
    }

    val (t2dElapsedTime, t2dMemUsed, trace2dSum) = performance {
      rdd2 map { _.result } reduce (_ + _)
    }

    println(s"rddElapsedTime=${rddElapsedTime}")
    println(s"rdd2ElapsedTime=${rdd2ElapsedTime}")
    println(s"t3dElapsedTime=${t3dElapsedTime}")
    println(s"t2dElapsedTime=${t2dElapsedTime}")
    spark.stop()
  }
}
