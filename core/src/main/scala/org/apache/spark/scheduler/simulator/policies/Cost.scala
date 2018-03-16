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

import scala.collection.mutable.{LinkedHashMap, PriorityQueue}
import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.ActiveJob
import org.apache.spark.scheduler.simulator.{Simulation, Simulator}

abstract class Cost extends Policy {
  override private[simulator] val name: String = "Cost"

  private val entries = new LinkedHashMap[RDD[_], CostContent]

  private val queue = new mutable.PriorityQueue[CostContent]()(
    Ordering.by(costContent => costContent.cost)
  )

  override private[simulator] def init(_simulation: Simulation): Unit = {
  }

  override private[simulator] def initJob(_job: ActiveJob): Unit = {
  }

  override private[simulator] def printEntries: String = {
    ""
  }

  /** Get the block from its id */
  override private[simulator] def get(rdd: RDD[_],
                             lastCachedRDD: Option[RDD[_]] = None): Option[Content] = {
    // Monads to the rescue.
//    for {
//      c <- entries.get(rdd)
//      c.addNeeded(lastCachedRDD)
//      c
//    } yield c.content
    None
  }

  /**
   * Insert a block. When calling this function, we must be sure that the size of the content
   * is not bigger than the total size of the memory.
   */
  override private[simulator] def put(rdd: RDD[_], content: Content,
                             lastCachedRDD: Option[RDD[_]] = None): Unit = {
    val cst = new CostContent(lastCachedRDD, content)
    entries.put(rdd, cst)
  }

  override private[simulator] def evictBlocksToFreeSpace(space: Double): Double

}

class CostContent(val last: Option[RDD[_]], cont: Content) {
  val neededBy: mutable.HashMap[Option[RDD[_]], Int] = new mutable.HashMap[Option[RDD[_]], Int]
  val cost: Long = 0L
  neededBy(last) = 1

  private[policies] val content = cont

  private[simulator] def addNeeded(n: Option[RDD[_]]): Unit = {
    neededBy(n) += 1
  }
}
