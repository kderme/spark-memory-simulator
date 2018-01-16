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

import org.apache.spark.scheduler.simulator.SizeAble

// The "<: SizeAble" is a type constraint that ensures that we can find the size of
// the content C by applying getSize.
class LRU[C <: SizeAble] (private[simulator] val isItLRU: Boolean) extends Policy[C]  {

  private[simulator] def this() = {
    this(true)
  }

  /** LinkedHashMap works like FIFO if isItLRU = false and like LRU if isItLRU = true */
  private val entries = new LinkedHashMap[Int, C](32, 0.75f, isItLRU)

  override private[simulator] def get(blockId: Int): Option[C] = {
    Option(entries.get(blockId))
  }

  override private[simulator] def put(blockId: Int, content: C): Unit = {
    entries.put(blockId, content)
    return ()
  }

  /** This is like org.apache.spark.storage.memory.MemoryStore.evictBlocksToFreeSpace */
  override private[simulator] def evictBlocksToFreeSpace(space: Long): Long = {
    var freedMemory = 0L
    val iterator = entries.entrySet().iterator()
    val selectedBlocks = new ArrayBuffer[Int]
    while (freedMemory < space && iterator.hasNext) {
      val pair = iterator.next()
      val blockId = pair.getKey
      // this is where the type constraint is used.
      val size = pair.getValue.getSize
      selectedBlocks += blockId
      freedMemory += size
    }
    selectedBlocks.foreach { entries.remove(_) }
    freedMemory
  }
}
