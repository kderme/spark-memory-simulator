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

package org.apache.spark.scheduler.simulator.sizePredictors

import org.apache.spark.Dependency
import org.apache.spark.rdd.{RDD, SimInfos}

private[simulator] class FineGrained(defaultParts: Int,
                  defaultSize: Double) extends SizePredictor {

  override def name: String = "fine-" + defaultParts + "-" + defaultSize

/*
  def createFromParents(rdd: RDD[_], infos: Map[Dependency[_], SimInfos]): SimInfos = {
    if (rdd.dependencies.length > 0) {
      val deps = infos.keys
      val fatherPartitions = deps.map(_.rdd.getNumPartitions).sum
      val fatherParts = deps.map(_.rdd.simInfos(id).totalParts).sum
      val fatherSize = deps.map(_.rdd.simInfos(id).sizePerPart).sum
      val totalParts = (fatherParts * rdd.getNumPartitions) / fatherPartitions
      // TODO in cases like filter or first etc.
      val size = fatherSize / (1D * fatherParts)
      SimInfos(totalParts, size)
    }
    else {
      SimInfos(defaultParts(rdd), defaultSize(rdd))
    }
  }
*/

  def createFromParents(rdd: RDD[_], infos: Map[Dependency[_], SimInfos]): SimInfos = {
    SimInfos(defaultParts, defaultSize)
  }

  override private[simulator] def predict(rdd: RDD[_]): Unit = {
    def predictRec(rdd: RDD[_]): SimInfos = {
      if (!rdd.simInfos.contains(id)) {
        val infos: Map[Dependency[_], SimInfos] = rdd.dependencies.map( dep =>
          (dep, predictRec(dep.rdd))
        ).toMap
        val info = createFromParents(rdd, infos)
        rdd.simInfos(id) = info
        info
      }
      else {
        rdd.simInfos(id)
      }
    }
    predictRec(rdd)
    print("Predicted-------------------------------------------")
    print(rdd.simInfos(id))
    ()
  }
}
