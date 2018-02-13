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

import scala.collection.mutable.MutableList

import org.apache.spark.scheduler.Stage
import org.apache.spark.scheduler.simulator.{DefaultContent, Simulation, Simulator}
import org.apache.spark.storage.StorageLevel

abstract class Scheduler {

  // All nulls must be initiated at the initialization of Simulation.
  private[simulator] var getParents: Stage => List[Stage] = null

  private[simulator] var simulation: Simulation = null

  private[simulator] val sequence = new MutableList[Stage]()

  private[simulator] def submitStage(stage: Stage): Unit

  private[simulator] def submitTask(stage: Stage) = {
    sequence += stage
    simulation.submitTask(stage)
  }


}
