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

package org.apache.spark.scheduler.simulator.policies

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.ActiveJob
import org.apache.spark.scheduler.simulator.{SimInfo, Simulation, Simulator}
import org.apache.spark.storage.BlockId

trait Policy{

  private[simulator] val name: String

  // All nulls must be initiated at the initialization of Simulation.
  private[simulator] var simulator: Simulator = null

  private[simulator] var simulation: Simulation = null

  private[simulator] def init(_simulation: Simulation): Unit = {
    simulation = _simulation
  }

  private[simulator] def initJob(_job: ActiveJob): Unit = {
  }

  private[simulator] def printEntries: String = {
    ""
  }

  /** Get the block from its id */
  private[simulator] def get(rdd: RDD[_], lastCachedRDD: Option[RDD[_]]): Option[Content]

  /**
   * Insert a block. When calling this function, we must be sure that the size of the content
   * is not bigger than the total size of the memory.
   */
  private[simulator] def put(rdd: RDD[_], content: Content,
                             lastCachedRDD: Option[RDD[_]]): Unit

  private[simulator] def evictBlocksToFreeSpace(space: Double): Double
}

private[simulator] class Content (
  private[simulator] val sizePerPart: Double,
  private[simulator] val totalParts: Int,
  private[simulator] var parts: Int) {

  private[simulator] def this(totalParts: Int, sizePerPart: Double) = {
    this(sizePerPart, totalParts, totalParts)
  }

  private[simulator] def deleteParts(target: Double) : Double = {
    var freedMemory = 0D
    while (freedMemory < target && parts > 0) {
      parts -= 1
      freedMemory += sizePerPart
    }
    freedMemory
  }

  private [simulator] def getSize: Double = sizePerPart * parts
  private[simulator] def toCaseClass: SimInfo =
    SimInfo(parts, totalParts, sizePerPart)
}
