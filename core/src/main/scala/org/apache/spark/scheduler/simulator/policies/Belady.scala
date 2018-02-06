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

import scala.collection.mutable.{ArrayBuffer, HashSet, LinkedHashMap, MutableList}
import scala.language.existentials

import org.apache.spark.{NarrowDependency, ShuffleDependency}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ActiveJob, Stage}
import org.apache.spark.scheduler.simulator._
import org.apache.spark.scheduler.simulator.scheduler.SparkScheduler
import org.apache.spark.storage.StorageLevel

class Belady [C <: SizeAble] extends Policy[C] with Logging {

  val name = "Belady"

  private val entries: LinkedHashMap[RDD[_], C] = new LinkedHashMap[RDD[_], C]

  /** This queue has the order of the requests */
  private[simulator] var sequence: MutableList[RDD[_]] = null

  override private[simulator] def init(_simulation: Simulation): Unit = {
    simulation = _simulation
  }

  override private[simulator] def initJob(job: ActiveJob): Unit = {
    val simulaption = predictor
    simulaption.simulate(job, false)
    sequence = simulaption.getSequence

    simulator.log("  Predicted Sequence = " + sequence.map(_.id))
  }

  override private[simulator] def printEntries: String = {
    entries.map({case (rdd, c) => (rdd.id, c.getSize)}) + ""
  }

  /** Get a block. We should always get the predicted sequence. */
  override private[simulator] def get(rdd: RDD[_]) = {
//    if (sequence.head != rdd) {
//      throw new SimulationException("Expected " + sequence.head.id + "but got " + rdd.id)
//    }
    sequence = sequence.tail
    entries.get(rdd)
  }

  /** Insert a block */
  override private[simulator] def put(rdd: RDD[_], content: C): Unit = {
    entries.put(rdd, content)
  }

  override private[simulator] def evictBlocksToFreeSpace(target: Long) = {
    // stale includes things that are in memory but not in future sequence.
    val stale = entries.keySet.filter(!sequence.contains(_)).to[MutableList]
    // willBeUsed includes things that are in memory, in the order that they will be used.
    val willBeUsed = sequence.filter(entries.contains(_))
    val unique = new MutableList[RDD[_]]()
    willBeUsed.foreach { rdd =>
      if (!unique.contains(rdd)) {
        unique += rdd
      }
    }
    // the following reverse is the idea of Belady.
    val ordered = stale ++ unique.reverse
    assert(stale.intersect(willBeUsed).isEmpty,
      "Stale and willBeUsed Lists should Not intersect!")
    simulator.log(" &&  " + ordered.map(_.id) + "")
    val iterator = ordered.iterator
    select(iterator, target: Long)
  }

  private[simulator] def select(iterator: Iterator[RDD[_]], target: Long): Long = {
    var freedMemory = 0L
    val selectedBlocks = new ArrayBuffer[Selected]
    while (freedMemory < target && iterator.hasNext) {
      val rdd = iterator.next()
      // if it is already selected, that means that this rdd was dublicated in
      // the future sequence. No need to enter again (although it wouldn`t cause problems).
      if (!selectedBlocks.contains(rdd)) {
        entries.get(rdd) match {
        case Some(content) =>
          freedMemory += content.deleteParts(target - freedMemory)
          // we don'`t delete inside the loop as we have an iterator.
          if (content.parts == 0) selectedBlocks += Selected(rdd, true)
          else {
            // if something is not entirely wiped, it should be selected
            // to change the sequence, but not removed from entries.
            selectedBlocks += Selected(rdd, false)
            simulator.assert(
              freedMemory >= target, "Content is not empty but target was not reached")
          }
        case None =>
          throw new SimulationException("Everything in ordered list should be in entries")
        }
      }
    }
    removeAndChangeFuture(selectedBlocks)
    freedMemory
  }

  private def removeAndChangeFuture (selectedBlocks: ArrayBuffer[Selected]): Unit = {
    // maybe assert that selectedBlocks has no dublicates.
    simulator.log("   Selected blocks = " + selectedBlocks.map(_.rdd.id))
    selectedBlocks.foreach { selected =>
      val rdd = selected.rdd
      if(selected.remove) {
        entries.remove(rdd)
      }
      val (a, b) = sequence.span(_ != rdd)
      if (!b.isEmpty) {
        sequence = a ++ MutableList(b.head) ++ createSubsequence(rdd) ++ b.tail
      }
      simulator.log("  Predicted Sequence = " + sequence.map(_.id))
    }
  }

  private def createSubsequence(rdd: RDD[_]): MutableList[RDD[_]] = {
    val simulaption = predictor
    simulaption.compute(rdd)
    simulaption.getSequence
  }

  private def predictor: Simulation = {
    simulator.log("Predicting..")
    // This is a dummy policy we must give to the internal simulator.
    // The internal simulator has infinite memory so the policy will never be used.
    val internalPolicy = new Dummy[SizeAble]()
    // This memory is assumed to have infinite size.
    val internalMemory = new MemoryManager[SizeAble](Long.MaxValue, internalPolicy)
    // This is a simulation inside a simulation.
    // TODO find a way to take automatically the scheduler that the real simulation work
    // TODO (same implementation different instance)
    val simulaption = new Simulation(simulator, internalMemory, new SparkScheduler)
    // This copies the current memory state.
    entries.foreach(entry => internalPolicy.entries.put(entry._1, entry._2))
    // This copies the current disk state.
    simulaption.disk = simulation.disk.clone()
    // This copies the current completed rdds (that are implicitely cached).
    simulaption.completedRDDS = simulation.completedRDDS.clone()
    simulaption.real = false
    simulaption
  }
}

class Predictor[C <: SizeAble](
  simulator: Simulator,
  cached: HashSet[RDD[_]],
  completed: HashSet[RDD[_]]
  ) {

  /**
   * This is like org.apache.spark.storage.rdd.RDD.compute, which runs on Workers.
   */
  private def compute(rdd: RDD[_], sequence: MutableList[RDD[_]]): Unit = {
    for (dep <- rdd.dependencies) {
      dep match {
        case shufDep: ShuffleDependency[_, _, _] =>
          ()
        case narrowDep: NarrowDependency[_] =>
          iterator(narrowDep.rdd, sequence)
      }
    }
  }

  /**
   * This is like org.apache.spark.storage.rdd.RDD.iterator, which runs on Workers.
   */
  private def getOrCompute(rdd: RDD[_], sequence: MutableList[RDD[_]]) = {
    if (rdd.getStorageLevel != StorageLevel.NONE) {
      sequence += rdd
      if (!cached.contains(rdd)) {
        compute(rdd, sequence)
        cached += rdd
      }
    }
    else {
      compute(rdd, sequence)
    }
  }

  /**
   * This is like RDD.iterator.
   */
  private def iterator(rdd: RDD[_], sequence: MutableList[RDD[_]]) = {
    if (rdd.getStorageLevel != StorageLevel.NONE) {
      getOrCompute(rdd, sequence)
    } else {
      compute(rdd, sequence)
    }
  }

  private def runTask(stage: Stage, sequence: MutableList[RDD[_]]) = {
    // simulator.log("BB: run task " + stage.id)
    iterator(stage.rdd, sequence)
    completed += stage.rdd
  }

  /**
   * This is like org.apache.spark.storage.rdd.Task.runTask, which runs on Workers.
   */
  private def submitTask(stage: Stage, sequence: MutableList[RDD[_]]) = {
    // simulator.log("BB: run stage " + stage.id)
    if (!completed.contains(stage.rdd)) {
      runTask(stage, sequence)
    }
  }

  /**
   * This is like org.apache.spark.scheduler.Dagscheduler.submitStage, which runs on Master.
   */
  private[policies] def submitStage(stage: Stage, sequence: MutableList[RDD[_]]): Unit = {
    // simulator.log("BB: stage " + stage.id)
    val missing = simulator.getMissingParentStages(stage).sortBy(_.id)
    for (parent <- missing) {
      submitStage(parent, sequence)
    }
    submitTask(stage, sequence)
  }
}

case class Selected(rdd: RDD[_], remove: Boolean)
