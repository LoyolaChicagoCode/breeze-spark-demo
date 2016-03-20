// Copyright (C) 2016-Present by George K Thiruvathukal.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

  case class Data(result: Double, time: Time, space: Space, hostname: String)

  case class Config(
    dim: Option[Int] = Some(DEFAULT_DIMENSION),
    nodes: Option[Int] = Some(DEFAULT_NODES),
    partitions: Option[Int] = Some(DEFAULT_PARTITIONS),
    workload: Option[Int] = Some(DEFAULT_WORKLOAD),
    outputDir: Option[String] = Some(DEFAULT_OUTPUT_DIR)
  )

  // Use Breeze DenseMatrix for the layers in the 3D array.

  def getDoubleMatrix(dim: Int): DenseMatrix[Double] = DenseMatrix.fill[Double](dim, dim) { math.random }

  def sumOfTraces3D(slice: Int, dim: Int): Data = {
    val createPhase = performance { Array.fill(dim)(getDoubleMatrix(dim)) }
    val computePhase = performance { createPhase.result.map { matrix => trace(matrix) }.sum }

    Data(computePhase.result, createPhase.time + computePhase.time, createPhase.space,
      InetAddress.getLocalHost.getHostName)
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
    parser.parse(args, Config())
  }

  def main(args: Array[String]): Unit = go(args)

  // scalastyle:off check.length

  def go(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LinearAlgebra File I/O")
    val spark = new SparkContext(conf)

    val appConfig = parseCommandLine(args).get
    val dim = appConfig.dim.get
    val partitions = appConfig.partitions.get
    val nodes = appConfig.nodes.get
    val workload = appConfig.workload.get
    val outputDir = appConfig.outputDir.get

    val rddGenerationPhase = performance {
      spark.parallelize(1 to workload, partitions).map {
        slice => sumOfTraces3D(slice, dim)
      }
    }

    val rdd = rddGenerationPhase.result

    val rddReducePhase = performance {
      rdd map { _.result } reduce (_ + _)
    }

    //writeNodeUsageReport
    writePerformanceReport
    spark.stop

    // End of computation

    def writeNodeUsageReport() = {
      val nodeFileName = s"${outputDir}/nodes-dim=${dim}-nodes=${nodes}-partitions=${partitions}-workload=${workload}.txt"
      val nodeFileWriter = new PrintWriter(new File(nodeFileName))
      nodeFileWriter.println("Node Usage (on cluster)") // scalastyle:ignore
      val pairs = rdd.map(lc => (lc.hostname, 1))
      val counts = pairs.reduceByKey((a, b) => a + b)
      val nodesUsed = counts.collect() foreach nodeFileWriter.println // scalastyle:ignore
      nodeFileWriter.close
    }

    def writePerformanceReport() = {
      val paramNodes = Seq(
        <param name="dim"> { dim } </param>
        <param name="partitions"> { partitions } </param>
        <param name="nodes"> { nodes } </param>
        <param name="outputdir"> { outputDir } </param>
      )

      val resultNodes = Seq(
        <performance name="rdd generation time"> { rddGenerationPhase.time.toXML } </performance>
        <performance name="rdd reduce time"> { rddReducePhase.time.toXML } </performance>
      )
      val params = <parameters> { paramNodes } </parameters>
      val results = <results> { resultNodes } </results>
      val xmlDocument = <run> { params } { results } </run>
      val xmlFileName = f"$outputDir/perf-d$dim%04d-n$nodes%04d-p$partitions%04d-w$workload%04d.xml"
      val writer = new PrintWriter(new File(xmlFileName))
      val pprinter = new scala.xml.PrettyPrinter(80, 2) // scalastyle:ignore
      val prettyXML = pprinter.format(xmlDocument)
      writer.println(prettyXML) // scalastyle:ignore
      writer.close
    }
  }
  // scalastyle:on check.length
}
