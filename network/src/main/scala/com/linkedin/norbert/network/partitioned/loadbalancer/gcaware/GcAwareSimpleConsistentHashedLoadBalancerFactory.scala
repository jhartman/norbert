/*
 * Copyright 2009-2015 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.norbert.network.partitioned.loadbalancer.gcaware

import java.util.TreeMap

import com.linkedin.norbert.cluster.InvalidClusterException
import com.linkedin.norbert.network.common.Endpoint
import com.linkedin.norbert.network.partitioned.loadbalancer.{PartitionedLoadBalancerFactory, SimpleConsistentHashedLoadBalancer}
import com.linkedin.norbert.norbertutils.{ClockComponent, SystemClockComponent}

/**
 * Created by sishah on 6/26/15.
 */


/**
 * Extends SimpleConsistenHashedLoadBalancer to add GC-awareness.
 * Nodes are additionally filtered to exclude those that are currently garbage collecting.
 *
 * @param cycleTime: The time period (in milliseconds) in which each node in the data center undergoes
 *                   garbage collection exactly once
 * @param slotTime: The time (in milliseconds) for the nodes in one cluster to finish pending requests
 *                  (SLA time) + the time to garbage collect (GC time).
 *
 *
 *    Offset 2 ------------------------------->
 *
 *    Offset 0 --->                                                                     <- Slot time ->
 *
 *                <-------------|-------------|-------------|-------------|-------------|------------->
 *
 *                <- Cycle Time ---------------------------------------------------------------------->
 *
 *
 * A node n is currently undergoing garbage collection if:
 *         [ [currentTime % cycleTime] / slotTime ]  == n.offset
 */
class GcAwareSimpleConsistentHashedLoadBalancerFactory[PartitionedId](numReplicas: Int, hashFn: PartitionedId => Int, endpointHashFn: String => Int, cycleTime: Int, slotTime: Int) extends PartitionedLoadBalancerFactory[PartitionedId] {
  @throws(classOf[InvalidClusterException])
  def newLoadBalancer(endpoints: Set[Endpoint]): SimpleConsistentHashedLoadBalancer[PartitionedId] = {

    val wheel = new TreeMap[Int, Endpoint]

    endpoints.foreach { endpoint =>
      endpoint.node.partitionIds.foreach { partitionId =>
        (0 until numReplicas).foreach { r =>
          val node = endpoint.node
          var distKey = node.id + ":" + partitionId + ":" + r + ":" + node.url
          wheel.put(endpointHashFn(distKey), endpoint)
        }
      }
    }

    GcAwareSimpleConsistentHashedLoadBalancer(endpoints, wheel, hashFn, cycleTime, slotTime)
  }



  def getNumPartitions(endpoints: Set[Endpoint]) = {
    endpoints.flatMap(_.node.partitionIds).size
  }
}

object GcAwareSimpleConsistentHashedLoadBalancer {

  def apply[PartitionedId](endpoints: Set[Endpoint], wheel: TreeMap[Int, Endpoint], hashFn: PartitionedId => Int, cycleTime: Int, slotTime: Int) = {

    new GcAwareSimpleConsistentHashedLoadBalancer[PartitionedId](endpoints, wheel,hashFn,cycleTime,slotTime) with SystemClockComponent

  }


}

abstract class GcAwareSimpleConsistentHashedLoadBalancer[PartitionedId](endpoints: Set[Endpoint], wheel: TreeMap[Int, Endpoint], hashFn: PartitionedId => Int, cycleTime: Int, slotTime: Int)
        extends SimpleConsistentHashedLoadBalancer[PartitionedId](wheel, hashFn) with GcAwareLoadBalancerHelper {

  this: ClockComponent =>

  val gcCycleTime = cycleTime
  val gcSlotTime = slotTime

  validateOffsets(endpoints)

}

