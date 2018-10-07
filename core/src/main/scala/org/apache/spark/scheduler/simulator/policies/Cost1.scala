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

import scala.collection.mutable._

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD, SimInfos}
import org.apache.spark.scheduler.ActiveJob
import org.apache.spark.scheduler.simulator._

class Cost1 extends Policy with Logging {
  override private[simulator] val name = "Cost1"

  private val entries: LinkedHashMap[RDD[_], Content] = new LinkedHashMap[RDD[_], Content]

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
    val simulaption = predictor(false)
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

  def cost(rdd: RDD[_]): Int = {
    val costSimulation = predictor(true)
    costSimulation.compute(rdd, rdd.simInfos(simulation.id).totalParts, None, false)
    costSimulation.getCost
  }

  private def orderByCosts(list: MutableList[RDD[_]]): MutableList[RDD[_]] = {
    val costs = list.map(rdd => (rdd, cost(rdd))).sortBy(_._2).reverse
    simulation.log("      Costs = " + costs.map({case (q, w) => (q.id, w)}) + "")
    costs.map(_._1)
  }

  override private[simulator] def evictBlocksToFreeSpace(target: Double) = {
    // stale includes things that are in memory but not in future sequence.
    val stale = entries.keySet.filter(!sequence.contains(_)).to[MutableList]
    // willBeUsed includes things that are in memory, sorted by costs
    val willBeUsed = orderByCosts(sequence.filter(entries.contains(_)))
    val unique = new MutableList[RDD[_]]()
    willBeUsed.foreach { rdd =>
      if (!unique.contains(rdd)) {
        unique += rdd
      }
    }
    // reverse-reversed.
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
    val simulaption = predictor(false)
    simulaption.compute(rdd, rdd.simInfos(simulation.id).totalParts, None, false)
    simulaption.getSequence
  }

  private def predictor(addNeededInMemory: Boolean): Simulation = {
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
    if(addNeededInMemory) {
      val id = simulation.id
      sequence.foreach(rdd => {
        val simInfos: SimInfos = rdd.simInfos(id)
        internalPolicy.entries.put(rdd,
          new Content(simInfos.totalParts, simInfos.sizePerPart))
      })
    }
    // This copies the current disk state.
    simulaption.disk = simulation.disk.clone()
    // This copies the current completed rdds (that are implicitely cached).
    simulaption.completedRDDS = simulation.completedRDDS.clone()
    simulaption
  }
}
