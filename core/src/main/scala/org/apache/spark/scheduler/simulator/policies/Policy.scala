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

import org.apache.spark.scheduler.simulator.Simulator
import org.apache.spark.storage.BlockId

trait Policy[C] {

  /**
   * Some Policies may need initialization. For those that don't a dummy default
   * implementation is given
   */
  private[simulator] def init(sim: Simulator): Unit = {
  }

  /** Get the block from its id */
  private[simulator] def get(blockId: Int): Option[C]

  /** Insert a block */
  private[simulator] def put(blockId: Int, content: C): Unit

  private[simulator] def evictBlocksToFreeSpace(space: Long): Long

}

