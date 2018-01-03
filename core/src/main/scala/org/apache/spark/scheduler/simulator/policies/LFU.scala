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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler.simulator.SizeAble


class LFU[C <: SizeAble] extends Policy[C] {

  private val entries = new mutable.LinkedHashMap[Int, LFUContent[C]]()

  override private[simulator] def get(blockId: Int): Option[C] = {
    // entries.get(blockId).flatMap(_.content)
    entries.get(blockId) match {
        // make this one-liner somehow.
      case None => None
      case Some(a) =>
        a.frequency += 1
        Some(a.content)
    }
  }

  override private[simulator] def put(blockId: Int, content: C): Unit = {
    val a = new LFUContent[C](1, content)
    entries.put(blockId, a)
  }

  override private[simulator] def evictBlocksToFreeSpace(id: Int, space: Long) = {
    var freedMemory = 0L
    val selectedBlocks = new ArrayBuffer[Int]
    while (freedMemory < space && entries.nonEmpty) {
      val blockId = getLFU
      val size = entries.get(blockId).get.content.getSize
      selectedBlocks += blockId
      freedMemory += size
    }
    selectedBlocks.foreach { entries.remove(_) }
    freedMemory
  }

  /** Will return a invalid key if entries are empty */
  private def getLFU = {
    var key = 0
    var minFreq = Integer.MAX_VALUE
    for((k, entry) <- entries) {
      if (entry.frequency < minFreq) {
        minFreq = entry.frequency
        key = k
      }
    }
    key
  }
}

class LFUContent[C] (fr: Int, cont: C) {
  private[policies] var frequency = fr
  private[policies] val content = cont
}
