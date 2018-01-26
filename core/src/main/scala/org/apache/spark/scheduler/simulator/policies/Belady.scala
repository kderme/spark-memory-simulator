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

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, MutableList}

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ActiveJob, Stage}
import org.apache.spark.scheduler.simulator.{Simulator, SizeAble}
import org.apache.spark.storage.StorageLevel

class Belady [C <: SizeAble] extends Policy[C] {

  private[scheduler] var job: ActiveJob = null

  val entries: HashMap[RDD[_], C] = new HashMap[RDD[_], C]

  var simulator: Simulator = null

  /** This queue has the order of the requests */
  private[simulator] var sequence: MutableList[RDD[_]] = null

  override private[simulator] def init(_simulator: Simulator, _job: ActiveJob): Unit = {
    simulator = _simulator
    job = _job
    val predictor = new Predictor(simulator, new HashSet())
    sequence = new MutableList[RDD[_]]
    predictor.createSeqFromStage(job.finalStage, sequence)
  }

  /** Get the block from its id */
  override private[simulator] def get(rdd: RDD[_]) = {
    entries.get(rdd)
  }

  /** Insert a block */
  override private[simulator] def put(rdd: RDD[_], content: C): Unit = {
    entries.put(rdd, content)
  }

  override private[simulator] def evictBlocksToFreeSpace(space: Long) = {
    var freedMemory = 0L
    val iterator = sequence.reverse.iterator
    val selectedBlocks = new ArrayBuffer[RDD[_]]
    while (freedMemory < space && iterator.hasNext) {
      val rdd = iterator.next()
      entries.get(rdd) match {
        case Some(content) =>
          val size = content.getSize
          selectedBlocks += rdd
          freedMemory += size
        case None =>
      }
    }
    freedMemory
  }
}

class Predictor(
  simulator: Simulator,
  set: HashSet[RDD[_]]
  ) {

  /**
   * This is like org.apache.spark.scheduler.Dagscheduler.submitStage, which runs on Master.
   */
  private[policies] def createSeqFromStage(stage: Stage, sequence: MutableList[RDD[_]]): Unit = {
    val missing = simulator.getMissingParentStages(stage).sortBy(_.id)
    for (parent <- missing) {
      createSeqFromStage(parent, sequence)
    }
    doCreateSeqFromStage(stage, sequence)
  }

  /**
   * This is like org.apache.spark.storage.rdd.Task.runTask, which runs on Workers.
   */
  private def doCreateSeqFromStage(stage: Stage, sequence: MutableList[RDD[_]]) =
    createSeqFromRDD(stage.rdd, sequence)

  /**
   * This is like org.apache.spark.storage.rdd.RDD.iterator, which runs on Workers.
   */
  private def createSeqFromRDD(rdd: RDD[_], sequence: MutableList[RDD[_]]) = {

    if (rdd.getStorageLevel != StorageLevel.NONE) {
      sequence += rdd
      if (!set.exists(rdd == _)) {
        set.add(rdd)
        compute(rdd, sequence)
      }
    }
    else {
      compute(rdd, sequence)
    }
  }

  /**
   * This is like org.apache.spark.storage.rdd.RDD.compute, which runs on Workers.
   */
  private def compute(rdd: RDD[_], sequence: MutableList[RDD[_]]): Unit = {
    for (dep <- rdd.dependencies) {
      createSeqFromRDD(dep.rdd, sequence)
    }
  }
}
