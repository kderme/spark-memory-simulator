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

import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet, Stack}
import scala.collection.mutable.MutableList

import org.apache.spark.{NarrowDependency, ShuffleDependency}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ActiveJob, ShuffleMapStage, Stage}
import org.apache.spark.scheduler.simulator.policies.Policy
import org.apache.spark.scheduler.simulator.scheduler.{DFSScheduler, Scheduler, SparkScheduler}
import org.apache.spark.scheduler.simulator.sizePredictors._

/**
 * A Simulator is the module that talks with the DagScheduler.
 * The Simulator may have many Simulations i.e. each with different policy.
 * Each Simulation keeps track of each own memory and Simulations are totaly independent.
 */
private[scheduler] class Simulator (
    private[scheduler] val shuffleIdToMapStage: HashMap[Int, ShuffleMapStage],
    private val appName: String,
    private val schedulerConf: String,
    private val sizePredictorConf: String,
    private val policyConf: String,
    private val sizeConf: String)
  extends Logging {

  var simulationId = 0

  private def getAndIncrement: Int = {
    val id = simulationId
    simulationId += 1
    id
  }

  var jobs = new MutableList[ActiveJob]

  val schedulers: Array[String] = schedulerConf.split("_").distinct

  val sizePredictors: Array[String] = sizePredictorConf.split("_").distinct

  val policies: Array[String] = policyConf.split("_").flatMap{
    _ match {
      case "All" => List("LRU", "LFU", "FIFO", "Belady", "NotBelady", "LRC", "Random")
      case c => List(c)
    }
  }.distinct

  val sizes: Array[Double] = sizeConf.split("_").flatMap{ rule: String =>
    val ss = rule.split("-")
    (ss(0).toDouble to ss(1).toDouble by ss(2).toDouble).toList
  }.distinct

  // nested flatMap smell like Monads.
  val simulations = schedulers.flatMap(scheduler => sizePredictors.flatMap(
    sizePredictor => sizes.flatMap(size => policies.map(policy =>
    new Simulation(this, getAndIncrement, Utils.toSchedulers(scheduler),
      Utils.toSizePredictor(sizePredictor), size, Utils.toPolicy(policy), true))
  )))

  val validSimulations = mutable.Map[Simulation, Boolean]()
  simulations.foreach(validSimulations(_) = true)

  logStart(policies)

  /** Simulates a new job */
  private[scheduler] def submitJob(job: ActiveJob): Unit = {
    if (!jobs.isEmpty) {
      log("  ,")
    }
    val valids = simulations.filter(validSimulations(_))
    if(!valids.isEmpty) {
      val lastSimulation = valids.last
      valids.foreach { simulation =>
        if (validSimulations(simulation)) {
          val res = simulation.simulate(job, true)
          if (!res) {
            validSimulations(simulation) = false
          }
        }
        if (simulation != lastSimulation) {
          log("  ,")
        }
      }
    }
    jobs += job
  }

  /**
   * This is like Dagscheduler.getMissingParentStages which runs on Master.
   * We keep it here and not in the Simulation, because it does not depent on
   * the actual simulation/excecution but instead only on the dag.
   */
  private[simulator] def getParentStages(stage: Stage): List[Stage] = {
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
                missing += mapStage
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
   * This is like Dagscheduler.getOrCreateShuffleMapStage, which runs on Master.
   * Again this only depends on the dag.
   */
  private[scheduler] def getShuffleMapStage(
                                             shuffleDep: ShuffleDependency[_, _, _],
                                             firstJobId: Int): ShuffleMapStage = {
    // Stage should be found here. If not let it crash.
    shuffleIdToMapStage.get(shuffleDep.shuffleId).get
  }

  private[scheduler] def logStart(policies: Array[String]): Unit = {
    log("{")
    log("  \"appName\" : " + toJsonString(appName) + ",")
    log("  \"trying schedulers\" : " + toJsonString(schedulerConf) + ",")
    log("  \"trying size predictors\" : " + toJsonString(sizePredictorConf) + ",")
    log("  \"trying sizes\" : %s".format(sizes.mkString("[", ",", "],")))
    log("  \"trying policies\" : %s".format(policies.map(toJsonString).mkString("[", ",", "],")))
    log("  \"trying total simulations\" : " + simulations.length + ",")
    log("  \"simulations conf\" : {")
    simulations.foreach { sim =>
      sim.logStart
      if(sim.id != simulationId - 1) {
        log("    ,")
      }
    }
    log("  },")
    log("  \"simulations\" : [")
  }

  private[scheduler] def logFinish(numTotalJobs: Int) = {
    log("  ],")
    log("  \"Final Job Id\" : " + (numTotalJobs - 1))
    log("}")
  }

  private[scheduler] def log(msg: String) =
    logSimulation(msg )

  private[simulator] def assert(flag: Boolean, cause: String) = {
    if (!flag) throw new SimulationException(cause)
  }

  private[simulator] def toJsonString (str: String) =
    "\"" + str + "\""
}
