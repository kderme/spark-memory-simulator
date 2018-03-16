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
import org.apache.spark.scheduler.simulator._
import org.apache.spark.scheduler.simulator.scheduler.DFSScheduler

class LRC extends Policy with Logging {

  val name = "LRC"

  private[scheduler] var job: ActiveJob = null

  val entries = new LinkedHashMap[RDD[_], LRCContent]

  override private[simulator] def printEntries: String = {
    entries.map({case (rdd, c) => (rdd.id, (c.frequency, c.references, c.content.getSize))}) + ""
  }

  /** Each time a new job comes we delete the */
  override private[simulator] def initJob(_job: ActiveJob): Unit = {
    job = _job
    // We need also to do an internal simulaption here to see
    // which rdds will be used.
    val simulaption = predictor
    simulaption.simulate(job, false)
    val used = simulaption.getSequence.to[Set]
    entries.foreach { entry =>
      val rdd = entry._1
      val value: LRCContent = entry._2
      value.frequency = 0
      rdd.refCounters.get(job.jobId) match {
        case None => value.references = 0
        case Some(ref) =>
          if (used.contains(rdd)) value.references = ref
          else value.references = 0
      }
    }
    ()
  }

  /** Get the block from its id. But after updating its frequence. */
  override private[simulator] def get(rdd: RDD[_], lastCachedRDD: Option[RDD[_]]) = {
    entries.get(rdd) match {
      // make this one-liner somehow.
      case None => None
      case Some(a) =>
        a.frequency += 1
        Some(a.content)
    }
  }

  /** Insert a block */
  override private[simulator] def put(rdd: RDD[_], content: Content,
                                      lastCachedRDD: Option[RDD[_]]): Unit = {
    val a = new LRCContent(1, rdd.refCounters(job.jobId), content)
    entries.put(rdd, a)
  }

  override private[simulator] def evictBlocksToFreeSpace(target: Double) = {
    var freedMemory = 0D
    while (freedMemory < target && entries.nonEmpty) {
      getLRC match {
        case None => ()
        case Some(rdd) =>
          val content = entries.get (rdd).get.content
          freedMemory += content.deleteParts(target - freedMemory)
          if (content.parts == 0) entries.remove(rdd)
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

  private def predictor: Simulation = {
    logWarning("Predicting..")
    // This is a dummy policy we must give to the internal simulator.
    // The internal simulator has infinite memory so the policy will never be used.
    // In this dummy policy we cache nothing, to see all the available rdds.
    val internalPolicy = new DummyPolicy(false)
    // This is a simulation inside a simulation.
    val simulaption = new Simulation(simulator, simulation.id,
      Utils.toSchedulers(simulation.scheduler.name),
      new sizePredictors.DummySizePredictor, Double.MaxValue, internalPolicy, false)
    // This copies the current memory state.
    entries.foreach(entry => internalPolicy.entries.put(entry._1, entry._2.content))
    // This copies the current disk state.
    simulaption.disk = simulation.disk.clone()
    // This copies the current completed rdds (that are implicitely cached).
    simulaption.completedRDDS = simulation.completedRDDS.clone()
    simulaption
  }
}

// references must change for each job.
class LRCContent (fr: Int, ref: Int, cont: Content) {
  private[policies] var frequency = fr
  private[policies] var references = ref
  private[policies] val content = cont
}
