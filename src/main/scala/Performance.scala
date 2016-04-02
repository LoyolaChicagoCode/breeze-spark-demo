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

package cs.luc.edu
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

package object performance {

  case class Time(t: Double) {
    val nanoseconds = t.toLong
    val milliseconds = (t / 1.0e6).toLong

    // scalastyle:off
    def +(another: Time): Time = Time(t + another.t)
    // scalastyle:on

    override def toString(): String = f"Time(t=$t%.2f, ns=$nanoseconds%d, ms=$milliseconds%d)";

    def toXML(): xml.Elem = {
      val rawText = f"$t%.2f"
      val nsText = f"$nanoseconds%d"
      val msText = f"$milliseconds%d"
      <time raw={ rawText } nanoseconds={ nsText } milliseconds={ msText }/>
    }

    def toJSON(): org.json4s.JsonAST.JObject = {
      val rawText = f"$t%.2f"
      val nsText = f"$nanoseconds%d"
      val msText = f"$milliseconds%d"
      ("raw" -> rawText) ~ ("nanoseconds" -> nsText) ~ ("milliseconds" -> msText)
    }
  }

  case class Space(m: Long) {
    val memUsed = m.toDouble
    val memUsedGB = memUsed / math.pow(1024.0, 3)
    val totalMemory = Runtime.getRuntime.totalMemory
    val totalGB = totalMemory / math.pow(1024.0, 3)
    val freeMemory = Runtime.getRuntime.freeMemory
    val freeGB = totalMemory / math.pow(1024.0, 3)

    override def toString(): String = f"Space(memUsedGB=$memUsedGB%.2f, free=$freeGB%.2f, total=$totalGB%.2f)";
  }
  // time a block of Scala code - useful for timing everything!
  // return a Time object so we can obtain the time in desired units

  sealed abstract class Perf

  case class PerfData[T](time: Time, space: Space, result: T)

  def performance[R](block: => R): PerfData[R] = {
    val t0 = System.nanoTime()
    val m0 = Runtime.getRuntime.freeMemory
    // This executes the block and captures its result
    // call-by-name (reminiscent of Algol 68)
    val result = block
    val t1 = System.nanoTime()
    val m1 = Runtime.getRuntime.freeMemory
    val deltaT = t1 - t0
    val deltaM = m0 - m1
    PerfData(Time(deltaT), Space(deltaM), result)
  }
}
