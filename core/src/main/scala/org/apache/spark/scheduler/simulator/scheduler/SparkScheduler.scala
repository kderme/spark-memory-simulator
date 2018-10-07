/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.simulator.scheduler

import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.Stage

private[simulator] class SparkScheduler extends Scheduler with Logging {

  def name: String = "spark"

  override private[simulator] def submitStage(stage: Stage): Unit = {
    val trace = predictStageScheduling(stage)
    trace.foreach(submitTask(_))
  }

  private def predictStageScheduling(stage: Stage): ListBuffer[Stage] = {
    val waiting = new ListBuffer[Stage]
    val running = new ListBuffer[Stage]
    val trace = new ListBuffer[Stage]

    def submit(stage: Stage, stack: String): Unit = {
      //      logWarning(stack + "virtual submit stage : " + stage.id)
      //      logWarning("State = " + waiting + "," + running + "" + trace)
      if (!waiting.contains(stage) && !running.contains(stage) && !trace.contains(stage)) {
        val missing = getParents(stage).sortBy(_.id)
        val reallyMissing = missing diff trace
        if (reallyMissing.isEmpty) {
          running += stage
        } else {
          for (parent <- missing) {
            submit(parent, stack + "  ")
          }
          waiting += stage
        }
      }
    }

    submit(stage, "")

    // here we assume that stages finish in FIFO.
    while (!running.isEmpty) {
      val done = running.remove(0)
      trace += done
      waiting.filter(_.parents.contains(done)).foreach { st =>
        waiting -= st
        submit(st, "")
      }
    }
  trace
  }
}
