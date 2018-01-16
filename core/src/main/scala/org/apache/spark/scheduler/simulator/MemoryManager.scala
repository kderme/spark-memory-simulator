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

import java.util.LinkedHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.simulator.policies._
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryEntry

/*
 * C is the parametric content of a block, which has the constraint SizeAble.
 * This ensures that no matter what the content is, we must be able to take the
 * size of the block from it.
 */
private[scheduler]
class MemoryManager[C <: SizeAble](
   private var maxMemory: Long,
   private[simulator] val policy: Policy[C]
 ) extends Logging {

  /** The size of the used storage memory */
  private var memoryUsed: Long = 0L

  private[simulator] def get(id: Int): Option[C] = policy.get(id)

  private[simulator] def put(id: Int, content: C): Boolean = {
    val size = content.getSize
    if (!fits(size)) {
      val evicted = policy.evictBlocksToFreeSpace(id, size)
      memoryUsed -= evicted
      if (!fits(size)) {
        logError("Evicted but still doesn`t fit!")
        false
      }
    }
    policy.put(id, content)
    memoryUsed += size
    true
  }

  private def fits(space: Long): Boolean = {
    space <= maxMemory - memoryUsed
  }
}

private[simulator] trait SizeAble {
  private[simulator] def getSize: Long
}

private[simulator] class DefaultContent(a: Long) extends SizeAble {
  val v = a

  override private[simulator] def getSize = v
}
