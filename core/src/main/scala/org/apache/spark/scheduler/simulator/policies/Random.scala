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

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

class Random (isBelady: Boolean = true) extends Policy with Logging {

  val name = "Random"

  val entries = new mutable.LinkedHashMap[RDD[_], Content]

  /** Get the block from its id */
  private[simulator] def get(rdd: RDD[_], lastCachedRDD: Option[RDD[_]]): Option[Content] = {
    entries.get(rdd)
  }

  /**
   * Insert a block. When calling this function, we must be sure that the size of the content
   * is not bigger than the total size of the memory.
   */
  private[simulator] def put(rdd: RDD[_], content: Content,
                             lastCachedRDD: Option[RDD[_]]): Unit = {
    entries.put(rdd, content)
    return ()
  }

  private[simulator] def evictBlocksToFreeSpace(target: Double): Double = {
    var freedMemory = 0D
    while (freedMemory < target && entries.nonEmpty) {
      val r = new scala.util.Random()
      val rdds = entries.keySet.toArray
      val rdd = rdds(r.nextInt(rdds.size))
      val content = entries.get(rdd).get
      freedMemory += content.deleteParts(target - freedMemory)
      if (content.parts == 0) entries.remove(rdd)
    }
    freedMemory
  }
}
