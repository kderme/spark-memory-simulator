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

import scala.collection.mutable.{HashSet}

import org.apache.spark.{NarrowDependency, ShuffleDependency}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ActiveJob, Stage}
import org.apache.spark.storage.StorageLevel

private[simulator] class Simulation (
  simulator: Simulator,
  memory: MemoryManager[SizeAble]) extends Logging {

  // rdds that are the results of Stages are cache implicitely in disk by spark.
  // We keep track of them as some Stages may be skipped
  // More:
  // stackoverflow.com/questions/34580662/what-does-stage-skipped-mean-in-apache-spark-web-ui
  // Spark equivalently uses Stage.findMissingPartitions in DagScheduler.submitMissingTasks
  private[simulator] val completedRDDS = new HashSet[RDD[_]]

  private[simulator] def copyCompleted = {
    completedRDDS.clone
  }

  private var activeJob: ActiveJob = null
  /* We assume infinite disk size */
  private val disk: HashSet[Int] = new HashSet[Int]

  private var hits = 0

  private var misses = 0

  private var diskHits = 0

  private var narrowDependencies = 0

  private var shuffleDpendencies = 0

  memory.policy.init(this)

  private[scheduler] def run(job: ActiveJob) = {
    activeJob = job
    simulator.log("{")
    simulator.log("  jobid = " + job.jobId)
    simulator.log("  policy = " + memory.policy.name)

    // Some Policies have special needs before starting a job.
    // For example Belady needs to make a prediction of the pattern.
    memory.policy.initJob(job)

    submitStage(job.finalStage)

    simulator.log("  Incremental Results = {")
    simulator.log("    hits = " + hits)
    simulator.log("    misses = " + misses)
    simulator.log("    diskHits = " + diskHits)
    simulator.log("    narrowDependencies = " + narrowDependencies)
    simulator.log("    shuffleDpendencies = " + shuffleDpendencies)
    simulator.log("    entries = " + memory.printEntries)
    simulator.log("    memory used = " + memory.memoryUsed)
    simulator.log("  }")
    simulator.log("}")
  }

  // Below things that run on EXECUTOR.

  /**
   * This is like RDD.compute.
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
   * This is like BlockManager.getOrCompute.
   * This is the function that actually uses the memory (get/put).
   */
  private def getOrCompute(rdd: RDD[_]): Unit = {
    if (rdd.getStorageLevel.useMemory) {
      memory.get(rdd) match {
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
      val size = rdd.dependencies.size.toLong
      memory.put(rdd, new DefaultContent(1))
    }
    if (rdd.getStorageLevel.useDisk) {
      disk.add(rdd.id)
    }
  }

  /**
   * This is like RDD.iterator.
   */
  private def iterator(rdd: RDD[_]) = {
    if (rdd.getStorageLevel != StorageLevel.NONE) {
      getOrCompute(rdd)
    } else {
      compute(rdd)
    }
  }

  /**
   * This is like Task.runTask.
   */
  private def runTask(stage: Stage): Boolean = {
    iterator(stage.rdd)
    completedRDDS.add(stage.rdd)
  }

  // Below things that run on MASTER

  /**
   * This simulates DagScheduler.submitMissingTasks.
   * Before actualy running a Stage we check whether its rdd was already computed
   * by a different Stage (cached implicitely in disk).
   */
  private def submitTask(stage: Stage) = {
    if (completedRDDS.contains(stage.rdd)) {
//      simulator.log("&& skipping stage " + stage.id +" (rdd " + stage.rdd.id + " is completed)")
    }
    else {
      simulator.log("&& running stage " + stage.id)
      runTask(stage)
    }
  }

  /**
   * This is like Dagscheduler.submitStage.
   * Here we follow a DFS approach for the scheduling of the stages.
   */
  private def submitStage(stage: Stage) {
    val missing = simulator.getMissingParentStages(stage).sortBy(_.id)
    for (parent <- missing) {
      submitStage(parent)
    }
    submitTask(stage)
  }
}
