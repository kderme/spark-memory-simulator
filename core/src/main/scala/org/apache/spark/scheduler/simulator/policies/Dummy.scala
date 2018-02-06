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

import scala.collection.mutable.LinkedHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.simulator.{SimulationException, SizeAble}

class Dummy [C <: SizeAble] (cache: Boolean = true) extends Policy[C] with Logging {

  private[simulator] val entries: LinkedHashMap[RDD[_], C] = new LinkedHashMap[RDD[_], C]

  override private[simulator] val name = "Dummmy"

  /** Get the block from its id */
  override private[simulator] def get(rdd: RDD[_]) = entries.get(rdd)

  /** Insert a block. */
  override private[simulator] def put(rdd: RDD[_], content: C): Unit =
    if (cache) {
      entries.put(rdd, content)
    }

  override private[simulator] def evictBlocksToFreeSpace(space: Long) = {
    throw new SimulationException("Dummy policy should never have to evict")
  }
}
