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

import java.util.LinkedHashMap

import scala.collection.mutable.{ArrayBuffer, HashSet, LinkedHashMap, MutableList}
import scala.collection.mutable
import scala.language.existentials

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ActiveJob, Stage}
import org.apache.spark.scheduler.simulator._
import org.apache.spark.scheduler.simulator.scheduler.{DFSScheduler, SparkScheduler}
import org.apache.spark.scheduler.simulator.sizePredictors.DummySizePredictor

class Belady (isBelady: Boolean = true, isLRU: Boolean = false) extends Policy with Logging {

  val name =
  if (isBelady) {
    "Belady"
  }
  else if (isLRU) {
    "BeladyLRU"
  }
  else {
    "RandomNeeded"
  }

  private val entries =
    new  scala.collection.mutable.LinkedHashMap[RDD[_], Content]

  /** scala.mutable.LinkedHashMap is FIFO. Here we simulate LRU */
  private[simulator] def get(rdd: RDD[_]): Option[Content] = {
    entries.remove(rdd) match {
      case None => None
      case Some(c) =>
        entries.put(rdd, c)
        Some(c)
    }
    // for {
    //   c <- entries.remove(rdd)
    //   _ <- entries.put(rdd, c)
    // } yield c
  }

  /** This queue has the order of the requests */
  private[simulator] var sequence: MutableList[RDD[_]] = null

  override private[simulator] def init(_simulation: Simulation): Unit = {
    simulation = _simulation
  }

  override private[simulator] def initJob(job: ActiveJob): Unit = {
    val simulaption = predictor
    simulaption.simulate(job, false)
    sequence = simulaption.getSequence

    simulation.log("  Predicted Sequence = " + sequence.map(_.id))
  }

  override private[simulator] def printEntries: String = {
    entries.map({case (rdd, c) => (rdd.id, c.toCaseClass)}) + ""
  }

  /** Get a block. We should always get the predicted sequence. */
  override private[simulator] def get(rdd: RDD[_], lastCachedRDD: Option[RDD[_]]) = {
//    if (sequence.head != rdd) {
//      throw new SimulationException("Expected " + sequence.head.id + "but got " + rdd.id)
//    }
    if (!sequence.isEmpty) {
      sequence = sequence.tail
    }
    get(rdd)
  }

  /** Insert a block */
  override private[simulator] def put(rdd: RDD[_], content: Content,
                                      lastCachedRDD: Option[RDD[_]]): Unit = {
    entries.put(rdd, content)
  }

  private def randomizeMaybe(list: MutableList[RDD[_]]): MutableList[RDD[_]] = {
    if (isBelady) {
      list
    }
    else if (isLRU) {
      val set = list.toSet
      val ls = new mutable.MutableList[RDD[_]]
      val it = entries.keysIterator
      for (rdd <- it) {
        if (set.contains(rdd)) {
          ls += rdd
        }
      }
      ls
    }
    else {
      scala.util.Random.shuffle(list)
    }
  }

  override private[simulator] def evictBlocksToFreeSpace(target: Double) = {
    // stale includes things that are in memory but not in future sequence.
    val stale = entries.keySet.filter(!sequence.contains(_)).to[MutableList]
    // willBeUsed includes things that are in memory, in the order that they will be used.
    val willBeUsed = randomizeMaybe(sequence.filter(entries.contains(_)))
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
    val iterator = ordered.iterator
    select(iterator, target: Double)
  }

  private[simulator] def select(iterator: Iterator[RDD[_]], target: Double): Double = {
    var freedMemory = 0D
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
    selectedBlocks.foreach { selected =>
      val rdd = selected.rdd
      if(selected.remove) {
        entries.remove(rdd)
      }
      val (a, b) = sequence.span(_ != rdd)
      if (!b.isEmpty) {
        sequence = a ++ MutableList(b.head) ++ createSubsequence(rdd) ++ b.tail
      }
    }
    simulation.log("  Predicted Sequence = " + sequence.map(_.id))
  }

  private def createSubsequence(rdd: RDD[_]): MutableList[RDD[_]] = {
    val simulaption = predictor
    simulaption.compute(rdd, rdd.simInfos(simulation.id).totalParts, None, false)
    simulaption.getSequence
  }

  private def predictor: Simulation = {
    logWarning("Predicting..")
    // This is a dummy policy we must give to the internal simulator.
    // The internal simulator has infinite memory so the policy will never be used.
    val internalPolicy = new DummyPolicy()
    // This is a simulation inside a simulation.
    val simulaption = new Simulation(simulator, simulation.id,
      Utils.toSchedulers(simulation.scheduler.name),
      new sizePredictors.DummySizePredictor, Double.MaxValue, internalPolicy, false)
    // This copies the current memory state.
    entries.foreach(entry => internalPolicy.entries.put(entry._1, entry._2))
    // This copies the current disk state.
    simulaption.disk = simulation.disk.clone()
    // This copies the current completed rdds (that are implicitely cached).
    simulaption.completedRDDS = simulation.completedRDDS.clone()
    simulaption
  }
}

case class Selected(rdd: RDD[_], remove: Boolean)
