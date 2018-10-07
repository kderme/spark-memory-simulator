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

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.Stage

class DFSScheduler extends Scheduler with Logging {

  def name: String = "dfs"

  val finished = new mutable.HashSet[Stage]

  override private[simulator] def submitStage (stage: Stage): Unit = {
    val parents1 = getParents(stage)
    assert(parents1 != null, "Null Parents")
    val parents = parents1.sortBy(_.id)
    for (parent <- parents) {
      if (simulation.completedRDDS.contains(parent.rdd)) {
        logWarning("  skipping stage " + stage.id +
          " and all fathers (rdd " + stage.rdd.id + " is completed)")
      }
      else {
        if (!finished.contains(parent)) {
          submitStage(parent)
        }
      }
    }
    submitTask(stage)
    finished += stage
  }
}
