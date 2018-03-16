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

import java.util.LinkedHashMap

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.simulator.{SimulationException}

// The "<: SizeAble" is a type constraint that ensures that we can find the size of
// the content C by applying getSize.
class LRU (private[simulator] val isItLRU: Boolean) extends  Policy {

  val name = "LRU"

  private[simulator] def this() = {
    this(true)
  }

  /** LinkedHashMap works like FIFO if isItLRU = false and like LRU if isItLRU = true */
  private val entries = new LinkedHashMap[Int, Content](32, 0.75f, isItLRU)

  override private[simulator] def printEntries: String = {
    var str = "["
    val iterator = entries.entrySet().iterator()
    while (iterator.hasNext) {
      val pair = iterator.next()
      val size = pair.getValue.getSize
      str = str + "(" + pair.getKey + ", " + pair.getValue.toCaseClass + ")"
      if (iterator.hasNext) {
        str += ", "
      }
    }
    str += "]"
    str
  }

  override private[simulator] def get(rdd: RDD[_],
                                      lastCachedRDD: Option[RDD[_]]): Option[Content] = {
    Option(entries.get(rdd.id))
  }

  override private[simulator] def put(rdd: RDD[_], content: Content,
                                      lastCachedRDD: Option[RDD[_]]): Unit = {
    entries.put(rdd.id, content)
    return ()
  }

  /** This is like org.apache.spark.storage.memory.MemoryStore.evictBlocksToFreeSpace */
  override private[simulator] def evictBlocksToFreeSpace(target: Double): Double = {
    var freedMemory = 0D
    val iterator = entries.entrySet().iterator()
    // these are the blocks, completely erased.
    val selectedBlocks = new ArrayBuffer[Int]
    while (freedMemory < target && iterator.hasNext) {
      val pair = iterator.next()
      val blockId = pair.getKey
      val content = pair.getValue
      assert(!selectedBlocks.contains(blockId), "Dublicated " + blockId)
      freedMemory += content.deleteParts(target - freedMemory)
      // we don'`t delete inside the loop as we have an iterator.
      if (content.parts == 0) selectedBlocks += blockId
      else if (freedMemory < target) {
        throw new SimulationException("content is not empty but target was not reached")
      }
    }
    selectedBlocks.foreach(entries.remove(_))
    freedMemory
  }
}
