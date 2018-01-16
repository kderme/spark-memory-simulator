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

import org.apache.spark.scheduler.simulator.SizeAble

class FIFO[C <: SizeAble] extends Policy[C] {

  private val lru = new LRU[C](false)

  override private[simulator] def get(blockId: Int) = {
    lru.get(blockId)
  }

  override private[simulator] def put(blockId: Int, content: C) = {
    lru.put(blockId, content)
  }

  override private[simulator] def evictBlocksToFreeSpace(space: Long) = {
    lru.evictBlocksToFreeSpace(space)
  }
}
