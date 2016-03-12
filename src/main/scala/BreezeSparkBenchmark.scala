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

  // This is the Scala way of doing a "struct". This allows us to change what is computed 
  // without having to change anything but countLinesInFile()

  case class Results(matrixTrace: Double, time: Time, space: Space)

  case class Config(dim: Option[Int] = None, slices: Int = 48)

  // This function is evaluated in parallel via the RDD

  def doSomeMath(slice: Int, dim: Int): Results = {

    // this is a function (closure)
    def getDoubleMatrix = DenseMatrix.fill[Double](dim, dim) { math.random }

    val (deltat, memUsed, matrixTrace) = performance {
      val a = Array.fill(dim)(getDoubleMatrix)
      a.map { matrix => trace(matrix) }.sum
    }
    Results(matrixTrace, deltat, memUsed)
  }

  def parseCommandLine(args: Array[String]): Option[Config] = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("breeze-spark-demo", "1.0")
      opt[Int]('d', "dim") action { (x, c) =>
        c.copy(dim = Some(x))
      } text ("dim is the matrix dimension (default = 2048)")
      help("help") text ("prints this usage text")

    }
    // parser.parse returns Option[C]
    parser.parse(args, Config())
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LinearAlgebra File I/O")
    val spark = new SparkContext(conf)
    val appConfig = parseCommandLine(args).getOrElse(Config())
    val dim = appConfig.dim.getOrElse(2048)
    val slices = appConfig.slices

    // create RDD from generated file listing

    val (rddElapsedTime, rddMemUsed, rdd) = performance {
      spark.parallelize(1 to slices, slices).map {
        slice => doSomeMath(slice, dim)
      }
    }

    val (traceElapsedTime, traceMemUsed, traceSum) = performance {
      rdd map { _.matrixTrace } reduce (_ + _)
    }

    println(s"rddElapsedTime=${rddElapsedTime}")
    println(s"traceElapsedTime=${traceElapsedTime}")
    spark.stop()
  }
}
