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

import org.apache.spark.scheduler.simulator.policies._
import org.apache.spark.scheduler.simulator.scheduler.{DFSScheduler, Scheduler}
import org.apache.spark.scheduler.simulator.sizePredictors.{EasyPredictor, FineGrained, SizePredictor}

private[simulator] object Utils {
  private[simulator] def toPolicy(policy: String): Policy = {
    policy match {
      case "LRU" => new LRU
      case "LFU" => new LFU
      case "FIFO" => new FIFO
      case "Belady" => new Belady
      case "LRC" => new LRC
      case "NotBelady" => new Belady(false)
      case "Random" => new Random
      case "Cost1" => new Cost1
    }
  }

  private[simulator] def toSchedulers(scheduler: String): Scheduler = {
    new DFSScheduler
  }

  private[simulator] def toSizePredictor(predictor: String): SizePredictor = {
    val conf = predictor.split("-")
    conf(0) match {
      case "easy" => new EasyPredictor()
      case "fine" => new FineGrained(conf(1).toInt, conf(2).toDouble)
    }
  }

}
