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

import scala.collection.mutable._

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.simulator.{SimulationException}

class LFU extends Policy {

  val name = "LFU"

  private val entries = new LinkedHashMap[Int, LFUContent]()

  override private[simulator] def get(rdd: RDD[_],
                                      lastCachedRDD: Option[RDD[_]]): Option[Content] = {
    // entries.get(blockId).flatMap(_.content)
    entries.get(rdd.id) match {
        // make this one-liner somehow.
      case None => None
      case Some(a) =>
        a.frequency += 1
        Some(a.content)
    }
  }

  override private[simulator] def put(rdd: RDD[_], content: Content,
                                      lastCachedRDD: Option[RDD[_]]): Unit = {
    val a = new LFUContent(1, content)
    entries.put(rdd.id, a)
  }

  override private[simulator] def evictBlocksToFreeSpace(target: Double) = {
    var freedMemory = 0D
    while (freedMemory < target && entries.nonEmpty) {
      val blockId = getLFU
      val content = entries.get(blockId).get.content
      freedMemory += content.deleteParts(target - freedMemory)
      // here we have to remove in the loop. Else getLFU would always give
      // the same result.
      if (content.parts == 0) entries.remove(blockId)
      else if (freedMemory < target) {
        throw new SimulationException("content is not empty but target was not reached")
      }
    }
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

  override private[simulator] def printEntries: String = {
    entries.map({case (rdd, c) => (rdd, c.content.toCaseClass)}) + ""
  }
}

class LFUContent (fr: Int, cont: Content) {
  private[policies] var frequency = fr
  private[policies] val content = cont
}
