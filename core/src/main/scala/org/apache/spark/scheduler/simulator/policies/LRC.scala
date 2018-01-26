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

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.ActiveJob
import org.apache.spark.scheduler.simulator.{Simulator, SizeAble}

class LRC [C <: SizeAble] extends Policy[C]{

  private[scheduler] var job: ActiveJob = null

  val entries: HashMap[Int, LRCContent[C]] = new HashMap[Int, LRCContent[C]]

  override private[simulator] def init(_simulator: Simulator, _job: ActiveJob): Unit = {
    job = _job
  }

  /** Get the block from its id */
  override private[simulator] def get(rdd: RDD[_]) = {
    entries.get(rdd.id) match {
      // make this one-liner somehow.
      case None => None
      case Some(a) =>
        a.frequency += 1
        Some(a.content)
    }
  }

  /** Insert a block */
  override private[simulator] def put(rdd: RDD[_], content: C): Unit = {
    val a = new LRCContent[C](1, rdd.refCounters(job.jobId), content)
    entries.put(rdd.id, a)
  }

  override private[simulator] def evictBlocksToFreeSpace(space: Long) = {
    var freedMemory = 0L
    val selectedBlocks = new ArrayBuffer[Int]
    while (freedMemory < space && entries.nonEmpty) {
      val blockId = getLRC
      val size = entries.get(blockId).get.content.getSize
      selectedBlocks += blockId
      freedMemory += size
    }
    selectedBlocks.foreach { entries.remove(_) }
    freedMemory
  }

  /** Will return a invalid key if entries are empty */
  private def getLRC = {
    var key = 0
    var minCount = Integer.MAX_VALUE
    for((k, entry) <- entries) {
      val future = entry.references - entry.frequency
      if (future < minCount) {
        minCount = future
        key = k
      }
    }
    key
  }
}

class LRCContent[C] (fr: Int, ref: Int, cont: C) {
  private[policies] var frequency = fr
  private[policies] val references = ref
  private[policies] val content = cont
}
