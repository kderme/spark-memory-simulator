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

package org.apache.spark.scheduler.simulator

import scala.collection.mutable.{HashMap, HashSet, Stack}

import org.apache.spark.{NarrowDependency, ShuffleDependency}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ActiveJob, DAGScheduler, ShuffleMapStage, Stage}
import org.apache.spark.scheduler.simulator.policies._
import org.apache.spark.storage.StorageLevel

private[scheduler]
class Simulator(
    private[scheduler] val job: ActiveJob,
    private[scheduler] val shuffleIdToMapStage: HashMap[Int, ShuffleMapStage],
    // This is inspired by, but has nothing to do with org.apache.spark.memory.MemoryManager.
    private[scheduler] val memory: MemoryManager[DefaultContent])
  extends Logging {

  def this(
            job: ActiveJob,
            shuffleIdToMapStage: HashMap[Int, ShuffleMapStage]
          ) = {
    this(job, shuffleIdToMapStage, new MemoryManager(10L, new Belady[DefaultContent](job)))
  }

  def this(
            dagScheduler: DAGScheduler,
            job: ActiveJob
          ) = {
    this(job, dagScheduler.shuffleIdToMapStage,
      new MemoryManager(10L, new Belady[DefaultContent](job)))
  }

  private var hits = 0

  private var misses = 0

  private var diskHits = 0

  private var narrowDependencies = 0

  private var shuffleDpendencies = 0

  /* We assume infinite disk size */
  private[scheduler] val disk = new HashSet[Int]

  private[scheduler] val waitingStages = new HashSet[Stage]

  private[scheduler] def run = {
    logSimulation("Starting for job = " + job.jobId)


    memory.policy.init(this)
    // Some Policies have special needs before starting.
    memory.policy match {
      case b : Belady[_] =>
          logSimulation("Belady Sequence: " + b.sequence.map(_.id))
      case _ => ()
    }
    submitStage(job.finalStage)

    logSimulation("Statistics for " + job.jobId + ":")
    logSimulation("hits = " + hits)
    logSimulation("misses = " + misses)
    logSimulation("diskHits = " + diskHits)
    logSimulation("narrowDependencies = " + narrowDependencies)
    logSimulation("shuffleDpendencies = " + shuffleDpendencies)
    logSimulation("Finished for job = " + job.jobId)
  }

  /**
   * This is like org.apache.spark.storage.rdd.RDD.compute, which runs on Workers.
   * A better simulation would take into consideration the implementation of compute for each RDD,
   * as RDD is an abstract class.
   */
  private def compute(rdd: RDD[_]): Unit = {
    for (dep <- rdd.dependencies) {
      dep match {
        case shufDep: ShuffleDependency[_, _, _] =>
          shuffleDpendencies += 1
        case narrowDep: NarrowDependency[_] =>
          narrowDependencies += 1
          iterator(narrowDep.rdd)
      }
    }
    // TODO. If an RDD is read by the filesystem or parallelized, its cost should be
    // counted here.
  }

  /**
   * This is like org.apache.spark.storage.rdd.Task.runTask, which runs on Workers.
   */
  private def runTask(stage: Stage) = iterator(stage.rdd)

  /**
   * This is like org.apache.spark.storage.rdd.RDD.iterator, which runs on Workers.
   */
  private def iterator(rdd: RDD[_]) = {
    if (rdd.getStorageLevel != StorageLevel.NONE) {
      getOrCompute(rdd)
    } else {
      compute(rdd)
    }
  }

  /**
   * This is like org.apache.spark.storage.BlockManager.getOrCompute, which runs on Workers.
   */
  private def getOrCompute(rdd: RDD[_]): Unit = {
    if (rdd.getStorageLevel.useMemory) {
      memory.get(rdd.id) match {
        case Some(block) =>
          hits += 1
          return
        case None =>
          misses += 1
      }
    }
    if (rdd.getStorageLevel.useDisk && disk.contains(rdd.id)) {
      diskHits += 1
      return
    }

    compute(rdd)

    if (rdd.getStorageLevel.useMemory) {
      // Assume for now size = 1
      memory.put(rdd.id, new DefaultContent(1L))
    }
    if (rdd.getStorageLevel.useDisk) {
      // This is not how it`s done in spark (when computed it`s only added in memory.
      // But we do it here that way for simplicity.
      disk.add(rdd.id)
    }
  }

  /**
   * This is like org.apache.spark.scheduler.Dagscheduler.submitStage, which runs on Master.
   */
  private def submitStage(stage: Stage) {
    logSimulation("submitStage(" + stage + ")")
    val missing = getMissingParentStages(stage).sortBy(_.id)
    logSimulation("missing: " + missing)
    for (parent <- missing) {
      submitStage(parent)
    }
    runTask(stage)
  }

  /**
   * This is like org.apache.spark.scheduler.Dagscheduler.getMissingParentStages,
   * which runs on Master.
   */
  private[simulator] def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]

    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              val mapStage = getShuffleMapStage(shufDep, stage.firstJobId)
              if (!mapStage.isAvailable) {
                missing += mapStage
              }
            case narrowDep: NarrowDependency[_] =>
              waitingForVisit.push(narrowDep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }

  /**
   * This is like org.apache.spark.scheduler.Dagscheduler.getOrCreateShuffleMapStage,
   * which runs on Master.
   */
  private[scheduler] def getShuffleMapStage(
       shuffleDep: ShuffleDependency[_, _, _],
       firstJobId: Int): ShuffleMapStage = {
    // Stage should be found here. If not let it crash.
    shuffleIdToMapStage.get(shuffleDep.shuffleId).get
  }

  private[scheduler] def logSimulation(msg: String): Unit = logWarning("|| SIMULATION || " ++ msg)

}

object Simulator {

}
