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
import org.apache.spark.scheduler.simulator.{DefaultContent, Simulator, SizeAble}
import org.apache.spark.storage.StorageLevel


class Belady [C <: SizeAble] (
    private[scheduler] val job: ActiveJob)
  extends Policy[C] {

  val entries: HashMap[Int, C] = new HashMap[Int, C]

  var simulator: Simulator = null

  /** This queue has the order of the requests */
  private[simulator] val sequence = new MutableList[RDD[_]]

  override private[simulator] def init(sim: Simulator): Unit = {
    simulator = sim
    val predictor = new Predictor(simulator, new HashSet())
    predictor.createSeqFromStage(job.finalStage, sequence)
  }

  /** Get the block from its id */
  override private[simulator] def get(blockId: Int) = {
    entries.get(blockId)
  }

  /** Insert a block */
  override private[simulator] def put(blockId: Int, content: C): Unit = {
    entries.put(blockId, content)
  }

  override private[simulator] def evictBlocksToFreeSpace(space: Long) = {
    var freedMemory = 0L
    val iterator = sequence.reverse.iterator
    val selectedBlocks = new ArrayBuffer[Int]
    while (freedMemory < space && iterator.hasNext) {
      val id = iterator.next().id
      entries.get(id) match {
        case Some(content) =>
          val size = content.getSize
          selectedBlocks += id
          freedMemory += size
        case None =>
      }
    }
    freedMemory
  }
}

private[policies] class Predictor (
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
  private def doCreateSeqFromStage(
    stage: Stage, sequence: MutableList[RDD[_]]) = createSeqFromRDD(stage.rdd, sequence)

  /**
   * This is like org.apache.spark.storage.rdd.RDD.iterator, which runs on Workers.
   */
  private def createSeqFromRDD(rdd: RDD[_], sequence: MutableList[RDD[_]]) = {

    if (rdd.getStorageLevel != StorageLevel.NONE) {
      sequence += rdd
      if (!set.exists(rdd == _)) {

        set.add(rdd)
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
