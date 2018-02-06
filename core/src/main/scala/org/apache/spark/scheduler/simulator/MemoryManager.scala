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

/*
 * C is the parametric content of a block, which has the constraint SizeAble.
 * This ensures that no matter what the content is, we must be able to take the
 * size of the block from it.
 */
private[scheduler]
class MemoryManager[C <: SizeAble](
   private[simulator] var maxMemory: Long,
   private[simulator] val policy: Policy[C]
 ) extends Logging {

  /** The size of the used storage memory */
  private[simulator] var memoryUsed: Long = 0L

  private[simulator] var sequence = new MutableList[RDD[_]]()

  private[simulator] def get(rdd: RDD[_]): Option[C] = {
    logWarning("Get " + rdd.id + " (" + memoryUsed + ")")
    sequence += rdd
    policy.get(rdd)
  }

  private[simulator] def put(rdd: RDD[_], content: C): Boolean = {
    logWarning("Put " + rdd.id + " (" + memoryUsed + ")")
    val size = content.getSize
    if (!fits(size)) {
      logWarning("not fit")
      if (oversized(size)) {
        // Should an exception be thrown here? Or just skip caching?
        throw new SimulationOufOfVirtualMemory(
          "rdd.id has size " + size + " while maxMemory = " + maxMemory)
      }
      val evicted = policy.evictBlocksToFreeSpace(size)
      if (evicted < size) {
        throw new SimulationException(
        " Policy " + policy.name + "evicted" + evicted + "instead of " + size)
      }
      memoryUsed -= evicted
    }
    policy.put(rdd, content)
    memoryUsed += size
    true
  }

  private[simulator] def printEntries: String = {
    policy.printEntries
  }

  private def fits(size: Long): Boolean = {
    size <= maxMemory - memoryUsed
  }

  private def oversized(size: Long): Boolean = {
    size > maxMemory
  }
}

abstract private[simulator] class SizeAble {
  private[simulator] val sizePerPart: Long
  private[simulator] val totalParts: Int
  private[simulator] var parts: Int
  private[simulator] def deleteParts(target: Long) : Long = {
    var freedMemory = 0L
    while (freedMemory < target && parts > 0) {
      parts -= 1
      freedMemory += sizePerPart
    }
    freedMemory
  }
  private [simulator] def getSize = sizePerPart * parts
  private[simulator] def toDefaultContent[C <: SizeAble](content: C): DefaultContent = {
    new DefaultContent(content.getSize)
  }
}

private[simulator] class DefaultContent (
  private [simulator] val sizePerPart: Long,
  private [simulator] val totalParts: Int,
  private [simulator] var parts: Int) extends SizeAble {

  def this(_size: Long) = this(_size, 1, 1)
}

private[simulator] class SimulationOufOfVirtualMemory(cause: String)
  extends Exception("Out of Virtual Memory: " + cause)

private[simulator] class SimulationException(cause: String)
  extends Exception(cause)

