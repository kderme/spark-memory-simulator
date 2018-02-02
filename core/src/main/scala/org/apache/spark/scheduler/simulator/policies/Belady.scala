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
import org.apache.spark.{NarrowDependency, ShuffleDependency}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ActiveJob, Stage}
import org.apache.spark.scheduler.simulator.{Simulation, SimulationException, Simulator, SizeAble}
import org.apache.spark.storage.StorageLevel

class Belady [C <: SizeAble](
    simulator: Simulator) extends Policy[C] with Logging {

  val name = "Belady"

  private var simulation: Simulation = null

  private[scheduler] var job: ActiveJob = null

  val entries: LinkedHashMap[RDD[_], C] = new LinkedHashMap[RDD[_], C]

  /** This queue has the order of the requests */
  private[simulator] var sequence: MutableList[RDD[_]] = null

  override private[simulator] def init(_simulation: Simulation): Unit = {
    simulation = _simulation
  }

  override private[simulator] def initJob(_job: ActiveJob): Unit = {
    job = _job
    val predictor = new Predictor(
      simulator,
      entries.keySet.to[HashSet],
      simulation.copyCompleted)
    sequence = new MutableList[RDD[_]]
    predictor.submitStage(job.finalStage, sequence)
    simulator.log("  Belady Sequence = " + sequence.map(_.id))
  }

  override private[simulator] def printEntries: String = {
    entries.map({case (rdd, c) => (rdd.id, c.getSize)}) + ""
  }

  /** Get a block. We should always get the predicted sequence. */
  override private[simulator] def get(rdd: RDD[_]) = {
    if (sequence.head != rdd) {
      throw new SimulationException("Expected " + sequence.head.id + "but got " + rdd.id)
    }
    sequence.drop(1)
    entries.get(rdd)
  }

  /** Insert a block */
  override private[simulator] def put(rdd: RDD[_], content: C): Unit = {
    entries.put(rdd, content)
  }

  override private[simulator] def evictBlocksToFreeSpace(space: Long) = {
    var freedMemory = 0L
    val selectedBlocks = new ArrayBuffer[RDD[_]]
    // We append the unused blocks with the sequence of used
    // and start evicting from the unused.
    val ls: MutableList[RDD[_]] = entries.keySet.filter(!sequence.contains(_)).to[MutableList]
    val ordered = ls ++ sequence
    simulator.log("  " + ordered.map(_.id) + "")
    val iterator = ordered.iterator
    while (freedMemory < space && iterator.hasNext) {
      val rdd: RDD[_] = iterator.next()
      entries.get(rdd) match {
        case Some(content) =>
          val size = content.getSize
          selectedBlocks += rdd
          freedMemory += size
        case None =>
      }
    }
    selectedBlocks.foreach { rdd =>
      // breaks the sequence in two.
      val (a, b) = sequence.span(_ != rdd)
      changeFuture(rdd.id)
      entries.remove(rdd)
    }
    freedMemory
  }

  /** TODO */
  private def changeFuture(id: Int): Unit = {
    simulator.log("Belady changing the future.")

    ()
  }
}

class Predictor(
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
      // simulator.log("SEQ: " + rdd.id)
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
