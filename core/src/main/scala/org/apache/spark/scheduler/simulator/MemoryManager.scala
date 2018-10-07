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

package org.apache.spark.scheduler.simulator

import scala.collection.mutable.MutableList

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.simulator.policies._

private[scheduler]
class MemoryManager(
   simulation: Simulation,
   private[simulator] var maxMemory: Double,
   private[simulator] val policy: Policy
 ) extends Logging {

  var id = -1

  /** The size of the used storage memory */
  private[simulator] var memoryUsed: Double = 0D

  private[simulator] var sequence = new MutableList[RDD[_]]()

  private[simulator] def get(rdd: RDD[_], lastCachedRDD: Option[RDD[_]]): Option[Content] = {
//    logWarning("Get " + rdd.id + " (" + memoryUsed + ")")
    simulation.log("    GET " + rdd.id + " " + rdd.simInfos.get(id))
    sequence += rdd
    policy.get(rdd, lastCachedRDD)
  }

  private[simulator] def put(rdd: RDD[_], content: Content,
                             lastCachedRDD: Option[RDD[_]]): Boolean = {
    val size = content.getSize
    simulation.log("    PUT " + rdd.id + " " + rdd.simInfos.get(id))
    printMemoryState()
    if (size > maxMemory - memoryUsed) {
      logWarning("not fit")
      if (oversized(size)) {
        // Should an exception be thrown here? Or just skip caching?
        // Spark skips caching, if a block is too big for memory.
        // throw new SimulationOufOfVirtualMemory(
        // "rdd.id has size " + size + " while maxMemory = " + maxMemory)
        printMemoryState()
        return false
      }
      val a = 0
      val evicted = policy.evictBlocksToFreeSpace(size - maxMemory + memoryUsed)
      memoryUsed -= evicted
      if (size > maxMemory - memoryUsed) {
        throw new SimulationException(
        " Policy " + policy.name + "evicted" + evicted + "instead of " +
          (size - maxMemory + memoryUsed))
      }
    }
    // if we reached here, rdd fits in memory.
    policy.put(rdd, content, lastCachedRDD)
    memoryUsed += size
    printMemoryState()
    true
  }

  private[simulator] def printEntries: String = {
    policy.printEntries
  }

  private def fits(size: Double): Boolean = {
    size <= maxMemory - memoryUsed
  }

  private def oversized(size: Double): Boolean = {
    size > maxMemory
  }

  private def printMemoryState(): Unit = {
    simulation.log("      STATE = " + memoryUsed + "/" + maxMemory)
    simulation.log("      ENTRIES = " + printEntries)
  }
}

case class SimInfo(parts: Int, totalParts: Int, sizePerPart: Double)

private[simulator] class SimulationOufOfVirtualMemory(cause: String)
  extends Exception("Out of Virtual Memory: " + cause)

private[simulator] class SimulationException(cause: String)
  extends Exception(cause)
