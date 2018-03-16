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

import scala.collection.mutable.{HashSet, MutableList}

import org.apache.spark.{NarrowDependency, ShuffleDependency}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ActiveJob, ShuffleMapStage, Stage}
import org.apache.spark.scheduler.simulator.policies.{Content, Policy}
import org.apache.spark.scheduler.simulator.scheduler.Scheduler
import org.apache.spark.scheduler.simulator.sizePredictors.SizePredictor
import org.apache.spark.storage.StorageLevel

private[simulator] class Simulation (
  private[simulator] val simulator: Simulator,
  private[simulator] val id: Int,
  private[simulator] val scheduler: Scheduler,
  private[simulator] val sizePredictor: SizePredictor,
  private[simulator] val size: Double,
  private[simulator] val policy: Policy,
  real: Boolean) extends Logging {

  val memory: MemoryManager = new MemoryManager(size, policy)

  memory.policy.simulator = simulator
  memory.policy.simulation = this
  memory.id = id
  scheduler.simulation = this
  scheduler.getParents = simulator.getParentStages
  sizePredictor.id = id

  var valid = true
  // rdds that are the results of Stages are cache implicitely in disk by spark.
  // We keep track of them as some Stages may be skipped
  // More:
  // stackoverflow.com/questions/34580662/what-does-stage-skipped-mean-in-apache-spark-web-ui
  // Spark equivalently uses Stage.findMissingPartitions in DagScheduler.submitMissingTasks
  private[simulator] var completedRDDS = new HashSet[RDD[_]]

  private var activeJob: ActiveJob = null
  /* We assume infinite disk size */
  private[simulator] var disk: HashSet[Int] = new HashSet[Int]

  private var hits = 0
  private var misses = 0
  private var diskHits = 0
  private var narrowDependencies = 0
  private var shuffleDpendencies = 0

  memory.policy.init(this)

  private[simulator] def getCost = {
    narrowDependencies + 10*shuffleDpendencies
  }

  /** call setId before simulate. */
  private[scheduler] def simulate(jobs: MutableList[ActiveJob], log: Boolean): Any = {
    val lastJob = jobs.last
    jobs.foreach { job =>
      simulate(job, job == lastJob)
    }
  }

  private[scheduler] def logStart: Unit = {
    if (real) {
      simulator.log("    \"" + id + "\" : {")
      simulator.log("      \"scheduler\" : " + simulator.toJsonString(scheduler.name) + ",")
      simulator.log("      \"size predictor\" : " +
        simulator.toJsonString(sizePredictor.name) + ",")
      simulator.log("      \"memory capacity\" : " + memory.maxMemory + ",")
      simulator.log("      \"policy\" : " + simulator.toJsonString(memory.policy.name))
      simulator.log("    }")
    }
  }

  private[scheduler] def simulate(job: ActiveJob, log: Boolean = true): Boolean = {
    if (!valid) {
      throw new SimulationException("Called invalidated simulation")
    }
    activeJob = job
    logWarning("Simulating job " + job.jobId)

    if(real) {
      // predict is recursive, so it should take care of all new rdds in this job.
      sizePredictor.predict(job.finalStage.rdd)
    }

    if (real) {
      logWarning("|| Memory: " + id + " ||JOB = " + job.jobId)
      simulator.log("  {")
      simulator.log("    \"simulation id\" : " + id + ",")
      simulator.log("    \"jobid\" : " + job.jobId + ",")
    }
    // Some Policies have special needs before starting a job.
    // For example Belady needs to make a prediction of the pattern.
    memory.policy.initJob(job)
    try {
      scheduler.submitStage(job.finalStage)
    }
    catch {
      case oovm: SimulationOufOfVirtualMemory =>
        logWarning(oovm.getMessage)
        simulator.log("    \"valid\": false " + ",")
        simulator.log("  }")
        valid = false
        return false
    }
    if (real) {
      simulator.log("    \"hits\" : " + hits + ",")
      simulator.log("    \"misses\" : " + misses + ",")
      simulator.log("    \"diskHits\" : " + diskHits + ",")
      simulator.log("    \"narrow dependencies\" : " + narrowDependencies + ",")
      simulator.log("    \"shuffled dependencies\" : " + shuffleDpendencies + ",")
      simulator.log("    \"valid\": true ")
      simulator.log("  }")
    }
    return true
  }

  private[simulator] def getSequence = {
    memory.sequence
  }

  // Below things that run on EXECUTOR.

  /**
   * This is like RDD.compute.
   * A better simulation would take into consideration the implementation of compute for each RDD,
   * as RDD is an abstract class.
   */
  private[simulator] def compute(rdd: RDD[_], threads: Int,
                                 lastCachedRDD: Option[RDD[_]], change: Boolean): Unit = {
    for (dep <- rdd.dependencies) {
      val newLastCachedRDD = if (change) Some(rdd) else lastCachedRDD
      dep match {
        case shufDep: ShuffleDependency[_, _, _] =>
          shuffleDpendencies += threads
        case narrowDep: NarrowDependency[_] =>
          narrowDependencies += threads
          // here we assume that all rdds have same size.
          iterator(narrowDep.rdd, threads, newLastCachedRDD)
      }
    }
    // TODO. If an RDD is read by the filesystem or parallelized, its cost should be
    // counted here.
  }

  /**
   * This is like BlockManager.getOrCompute.
   * This is the function that actually uses the memory (get/put).
   */
  private def getOrCompute(rdd: RDD[_], threads: Int, lastCachedRDD: Option[RDD[_]]): Unit = {
    assert(threads <= rdd.simInfos(id).totalParts,
      "More threads than blocks (" + threads + ", " + rdd.simInfos(id).totalParts + ")")
    assert(threads > 0)
    // threadsLeft left should be lower than threads.
    var threadsLeft = threads
    if (rdd.getStorageLevel.useMemory) {
      memory.get(rdd, lastCachedRDD) match {
        case Some(content) =>

          // mean value of hypergeometric distribution (link below broken in 2 lines).
          // https://en.wikipedia.org/wiki/Hypergeometric_distribution#Multivariate_
          // hypergeometric_distribution
          // TODO maybe round to closest ??
          val newHits = content.parts * threads / content.totalParts
          hits += newHits
          threadsLeft = threads - newHits
          misses += threadsLeft
          if (threadsLeft == 0) {
            // if we have 0 misses, no need to continue.
            return
          }
        case None =>
          // TODO
          misses += threads
      }
    }
    if (rdd.getStorageLevel.useDisk && disk.contains(rdd.id)) {
      diskHits += 1
      return
    }
    assert(threadsLeft <= threads, "Left more than came")
    assert(threadsLeft > 0)
    // The change of lastCachedRDD happens in compute.
    compute(rdd, threadsLeft, lastCachedRDD, true)

    if (rdd.getStorageLevel.useMemory) {
      val size = rdd.dependencies.size.toDouble
      // TODO approprate sizePerPart.
      memory.put(rdd, new Content(rdd.simInfos(id).totalParts, rdd.simInfos(id).sizePerPart),
        lastCachedRDD)
    }
    if (rdd.getStorageLevel.useDisk) {
      disk.add(rdd.id)
    }
  }

  /**
   * This is like RDD.iterator.
   */
  private def iterator(rdd: RDD[_], threads: Int, lastCachedRDD: Option[RDD[_]]) = {
    logWarning("|| Memory: " + id + " ||  iterator = " + rdd.id)
    if (rdd.getStorageLevel != StorageLevel.NONE) {
      getOrCompute(rdd, threads, lastCachedRDD)
    } else {
      compute(rdd, threads, lastCachedRDD, false)
    }
  }

  /**
   * This is like Task.runTask.
   */
  private def runTask(stage: Stage) = {
    val rdd = stage.rdd
    iterator(rdd, rdd.simInfos(id).totalParts, None)
    stage match {
      // only results of shufleMapStage are cached implicitely.
      case s: ShuffleMapStage => completedRDDS.add(stage.rdd)
      case s: Stage =>
    }
  }

  // Below things that run on MASTER

  /**
   * This simulates DagScheduler.submitMissingTasks.
   * Before actualy running a Stage we check whether its rdd was already computed
   * by a different Stage (cached implicitely in disk).
   */
  private[simulator] def submitTask(stage: Stage) = {
    if (completedRDDS.contains(stage.rdd)) {
        logWarning("  skipping stage " + stage.id +" (rdd " + stage.rdd.id + " is completed)")
    }
    else {
      logWarning("|| Memory: " + id + " ||  STAGE = " + stage.id)
      logWarning("|| Memory: " + id + " ||  STAGE RDD = " + stage.rdd.id)
      runTask(stage)
    }
  }
}
