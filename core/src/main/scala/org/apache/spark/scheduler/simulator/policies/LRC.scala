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

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.ActiveJob
import org.apache.spark.scheduler.simulator.{Simulator, SizeAble}

class LRC [C <: SizeAble] extends Policy[C] with Logging {

  val name = "LRC"

  private[scheduler] var job: ActiveJob = null

  val entries = new LinkedHashMap[RDD[_], LRCContent[C]]

  override private[simulator] def printEntries: String = {
    entries.map({case (rdd, c) => (rdd.id, (c.frequency, c.references, c.content.getSize))}) + ""
  }

  /** Each time a new job comes we delete the */
  override private[simulator] def initJob(_job: ActiveJob): Unit = {
    job = _job
    entries.foreach { entry =>
      val rdd = entry._1
      val value: LRCContent[C] = entry._2
      value.frequency = 0
      rdd.refCounters.get(job.jobId) match {
        case None => value.references = 0
        case Some(ref) =>
          value.references = ref
      }
    }
    ()
  }

  /** Get the block from its id. But after updating its frequence. */
  override private[simulator] def get(rdd: RDD[_]) = {
    entries.get(rdd) match {
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
    entries.put(rdd, a)
  }

  override private[simulator] def evictBlocksToFreeSpace(space: Long) = {
    var freedMemory = 0L
    while (freedMemory < space && entries.nonEmpty) {
      getLRC match {
        case None => ()
        case Some(rdd) =>
          val size = entries.get (rdd).get.content.getSize
          entries.remove (rdd)
          freedMemory += size
      }
    }
    freedMemory
  }

  /** Will return null if entries are empty */
  private def getLRC: Option[RDD[_]] = {
    var key: RDD[_] = null
    var minCount = Integer.MAX_VALUE
    for((rdd, entry) <- entries) {
      val future = entry.references - entry.frequency
      if (future < minCount) {
        minCount = future
        key = rdd
      }
    }
    Option(key)
  }
}

// references must change for each job.
class LRCContent[C] (fr: Int, ref: Int, cont: C) {
  private[policies] var frequency = fr
  private[policies] var references = ref
  private[policies] val content = cont
}
