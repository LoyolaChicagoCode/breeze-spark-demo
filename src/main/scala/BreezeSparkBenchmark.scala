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
import org.json4s._
import org.json4s.jackson._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

object BreezeSparkBenchmark {

  val DEFAULT_DIMENSION = 2048
  val DEFAULT_NODES = 4
  val DEFAULT_PARTITIONS = 48
  val DEFAULT_WORKLOAD = DEFAULT_NODES * DEFAULT_PARTITIONS
  val DEFAULT_OUTPUT_DIR = "./results"
  val DEFAULT_CACHE_POLICY = false
  val DEFAULT_XML = false
  val DEFAULT_JSON = false

  case class Data(result: Double, time: Time, space: Space, hostname: String)

  case class Config(
    dim: Option[Int] = Some(DEFAULT_DIMENSION),
    nodes: Option[Int] = Some(DEFAULT_NODES),
    partitions: Option[Int] = Some(DEFAULT_PARTITIONS),
    workload: Option[Int] = Some(DEFAULT_WORKLOAD),
    outputDir: Option[String] = Some(DEFAULT_OUTPUT_DIR),
    outputJson: Boolean = false,
    outputXML: Boolean = false
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

      opt[Unit]('j', "json") action { (_, c) =>
        c.copy(outputJson = true)
      } text (s"outputJson is whether to write JSON reports (default $DEFAULT_JSON)")

      opt[Unit]('x', "xml") action { (_, c) =>
        c.copy(outputXML = true)
      } text (s"outputXML is whether to write XML reports (default $DEFAULT_XML)")

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
      val rdd = spark.parallelize(1 to workload, partitions).map {
        slice => sumOfTraces3D(slice, dim)
      }
      rdd.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
      rdd
    }

    val rdd = rddGenerationPhase.result

    val rddReducePhase = performance {
      rdd map { _.result } reduce (_ + _)
    }

    if (appConfig.outputXML)
      writePerformanceReportXML
    if (appConfig.outputJson)
      writePerformanceReportJSON
    spark.stop

    // End of computation

    def computeNodeUsage: Array[(String, Int)] = {
      val pairs = rdd.map(lc => (lc.hostname, 1))
      val counts = pairs.reduceByKey((a, b) => a + b)
      counts.collect()
    }
    def nodeUsageXML: xml.Node = {
      <nodes>
        { computeNodeUsage map { case (hostname, count) => <node name={ hostname } workload={ count.toString }/> } }
      </nodes>
    }

    def nodeUsageJSON: org.json4s.JsonAST.JObject = {
      computeNodeUsage.foldLeft(JObject())(_ ~ _)
    }

    def writePerformanceReportXML() = {
      val document = <run>
                       <parameters>
                         <param name="dim" value={ dim.toString }/>
                         <param name="partitions" value={ partitions.toString }/>
                         <param name="workload" value={ workload.toString }/>
                         <param name="nodes" value={ nodes.toString }/>
                         <param name="outputdir" value={ outputDir.toString }/>
                       </parameters>
                       <results>
                         <performance name="rdd generation time">
                           { rddGenerationPhase.time.toXML }
                         </performance>
                         <performance name="rdd reduce time">
                           { rddReducePhase.time.toXML }
                         </performance>
                       </results>
                       { nodeUsageXML }
                     </run>

      val xmlFileName = f"$outputDir/perf-d$dim%04d-n$nodes%04d-p$partitions%04d-w$workload%04d.xml"
      val writer = new PrintWriter(new File(xmlFileName))
      val pprinter = new scala.xml.PrettyPrinter(80, 2) // scalastyle:ignore
      writer.println(pprinter.format(document)) // scalastyle:ignore
      writer.close
    }

    def writePerformanceReportJSON() = {
      val params = ("dim" -> dim) ~ ("partitions" -> partitions) ~
        ("workload" -> workload) ~ ("nodes" -> nodes) ~ ("outputDir" -> outputDir)

      val results = ("rdd generation time" -> rddGenerationPhase.time.toJSON) ~
        ("rdd reduce time" -> rddReducePhase.time.toJSON)

      val json = ("params" -> params) ~ ("results" -> results) ~ ("nodes" -> nodeUsageJSON)
      val jsonFileName = f"$outputDir/perf-d$dim%04d-n$nodes%04d-p$partitions%04d-w$workload%04d.json"
      val writer = new PrintWriter(new File(jsonFileName))
      writer.println(pretty(render(json)))
      writer.close
    }
  }
  // scalastyle:on check.length
}
