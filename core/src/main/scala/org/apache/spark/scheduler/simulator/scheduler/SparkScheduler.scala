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

import scala.collection.mutable.{HashSet, MutableList}

import org.apache.spark.scheduler.Stage

private[simulator] class SparkScheduler extends Scheduler {

  def name: String = "SparkScheduler"

  val waitingStages = new HashSet[Stage]
  var ready = new MutableList[Stage]

  override private[simulator] def submitStage (stage: Stage): Unit = {
    while (ready.isEmpty) {
      val stage = ready.head
      submitTask(stage)
      ready = ready.tail
    }

    val parents = getParents(stage).sortBy(_.id)
    for (parent <- parents) {
      submitStage(parent)
    }
    // sequence += stage
    ready += stage
  }
}
