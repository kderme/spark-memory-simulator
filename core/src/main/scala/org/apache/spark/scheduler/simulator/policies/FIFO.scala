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

import org.apache.spark.rdd.RDD

class FIFO extends Policy {

  val name = "FIFO"

  // LRU uses internally LinkedHashMap. This struct can be used as FIFO, given a false flag.
  private val lru = new LRU(false)

  override private[simulator] def printEntries: String = {
    lru.printEntries
  }

  override private[simulator] def get(rdd: RDD[_], lastCachedRDD: Option[RDD[_]]) = {
    lru.get(rdd, lastCachedRDD)
  }

  override private[simulator] def put(rdd: RDD[_], content: Content,
                                      lastCachedRDD: Option[RDD[_]]) = {
    lru.put(rdd, content, lastCachedRDD)
  }

  override private[simulator] def evictBlocksToFreeSpace(space: Double) = {
    lru.evictBlocksToFreeSpace(space)
  }
}
