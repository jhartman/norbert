/*
 * Copyright 2009-2010 LinkedIn, Inc
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
package com.linkedin.norbert
package cluster
package zookeeper

import common.ClusterNotificationManagerComponent
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import actors.Actor
import Actor._
import org.specs.util.WaitFor
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper._
import java.util.ArrayList
import java.util

class ZooKeeperClusterManagerComponentSpec extends SpecificationWithJUnit with Mockito with WaitFor with ZooKeeperClusterManagerComponent
        with ClusterNotificationManagerComponent {
  import ZooKeeperMessages._
  import ClusterManagerMessages._

  val mockZooKeeper = mock[ZooKeeper]

  var connectedCount = 0
  var disconnectedCount = 0
  var nodesChangedCount = 0
  var shutdownCount = 0
  var nodesReceived: Set[Node] = Set()

  def zkf(connectString: String, sessionTimeout: Int, watcher: Watcher) = mockZooKeeper
  val clusterManager = new ZooKeeperClusterManager("", 0, "test")(zkf _)

  val rootNode = "/test"
  val membershipNode = rootNode + "/members"
  val availabilityNode = rootNode + "/available"

  val clusterNotificationManager = actor {
    loop {
      react {
        case ClusterNotificationMessages.Connected(nodes) => connectedCount += 1; nodesReceived = nodes
        case ClusterNotificationMessages.Disconnected => disconnectedCount += 1
        case ClusterNotificationMessages.NodesChanged(nodes) => nodesChangedCount += 1; nodesReceived = nodes
        case ClusterNotificationMessages.Shutdown => shutdownCount += 1
      }
    }
  }

  "ZooKeeperClusterManager" should {
    doBefore {
      clusterManager.start
    }
    doAfter {
      clusterManager ! Shutdown
    }

    "instantiate a ZooKeeper instance when started" in {
      var callCount = 0
      def countedZkf(connectString: String, sessionTimeout: Int, watcher: Watcher) = {
        callCount += 1
        mockZooKeeper
      }

      val zkm = new ZooKeeperClusterManager("", 0, "")(countedZkf _)
      zkm.start

      callCount must eventually(be_==(1))

      zkm ! Shutdown
    }

    "when a Connected message is received" in {
      "verify the ZooKeeper structure by" in {
        val znodes = List(rootNode, membershipNode, availabilityNode)

        "doing nothing if all znodes exist" in {
          znodes.foreach(mockZooKeeper.exists(_, false) returns mock[Stat])

          clusterManager ! Connected
          waitFor(10.ms)

          znodes.foreach(there was one(mockZooKeeper).exists(_, false))
        }

        "creating the cluster, membership and availability znodes if they do not already exist" in {
          znodes.foreach { path =>
            mockZooKeeper.exists(path, false) returns null
            mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) returns path
          }

          clusterManager ! Connected
          waitFor(10.ms)

          znodes.foreach { path =>
            there was one(mockZooKeeper).exists(path, false)
            there was one(mockZooKeeper).create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
          }
        }
      }

      "calculate the current nodes" in {
        val membership = new ArrayList[String]
        membership.add("1")
        membership.add("2")
        membership.add("3")

        val availability = membership.clone.asInstanceOf[ArrayList[String]]
        availability.remove(2)

        val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
          Node(2, "localhost:31314", false, Set(2, 3)), Node(3, "localhost:31315", true, Set(2, 3)))

        mockZooKeeper.getChildren(membershipNode, true) returns membership
        nodes.foreach { node =>
          mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), clusterManager.getWatcher, null) returns Node.nodeToByteArray(node)
        }
        mockZooKeeper.getChildren(availabilityNode, true) returns availability

        clusterManager ! Connected
        waitFor(50.ms)

        got {
          one(mockZooKeeper).getChildren(membershipNode, true)
          nodes.foreach {node =>
            one(mockZooKeeper).getData("%s/%d".format(membershipNode, node.id), clusterManager.getWatcher, null)
          }
          one(mockZooKeeper).getChildren(availabilityNode, true)
        }
      }

      "send a notification to the notification manager actor" in {
        val membership = new ArrayList[String]
        membership.add("1")
        membership.add("2")
        membership.add("3")

        val availability = membership.clone.asInstanceOf[ArrayList[String]]
        availability.remove(1)

        val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
          Node(2, "localhost:31314", false, Set(2, 3)), Node(3, "localhost:31315", true, Set(2, 3)))

        mockZooKeeper.getChildren(membershipNode, true) returns membership
        nodes.foreach { node =>
          mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), clusterManager.getWatcher, null) returns Node.nodeToByteArray(node)
        }
        mockZooKeeper.getChildren(availabilityNode, true) returns availability

        clusterManager ! Connected

        connectedCount must eventually(be_==(1))
        nodesReceived.size must be_==(3)
        nodesReceived must containAll(nodes)
        nodesReceived.foreach { node => node must be_==(nodes(node.id - 1)) }
      }
    }

    "when a Disconnected message is received" in {
      "send a notification to the notification manager actor" in {
        clusterManager ! Connected
        clusterManager ! Disconnected

        disconnectedCount must eventually(be_==(1))
      }

      "do nothing if not connected" in {
        clusterManager ! Disconnected

        disconnectedCount must eventually(be_==(0))
      }
    }

    "when an Expired message is received" in {
      "reconnect to ZooKeeper" in {
        var callCount = 0
        def countedZkf(connectString: String, sessionTimeout: Int, watcher: Watcher) = {
          callCount += 1
          mockZooKeeper
        }

        val zkm = new ZooKeeperClusterManager("", 0, "")(countedZkf _)
        zkm.start
        zkm ! Connected
        zkm ! Expired

        callCount must eventually(be_==(2))

        zkm ! Shutdown
      }
    }

    "when a NodeChildrenChanged message is received" in {
      "and the membership node changed" in {
        "update the node capability" in {
          //TODO make sure the capability has changed
          val membership = new ArrayList[String]
          membership.add("1")

          val availability = membership.clone.asInstanceOf[ArrayList[String]]
          val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2), Some(0L), Some(5L)))

          val membershipUpdated = new ArrayList[String]
          membershipUpdated.add("1")
          membershipUpdated.add("2")
          val availabilityUpdated = membershipUpdated.clone().asInstanceOf[util.ArrayList[String]]
          val nodesUpdated = Array(Node(1, "localhost:31313", true, Set(1, 2), Some(0L), Some(6L)), Node(2, "localhost:31323", true, Set(3,4), Some(0L), Some(7L)))

          mockZooKeeper.getChildren(membershipNode, true) returns membership thenReturns membershipUpdated
          mockZooKeeper.getData("%s/%d".format(membershipNode, 1), clusterManager.getWatcher, null) returns (Node.nodeToByteArray(nodes(0))) thenReturns (Node.nodeToByteArray(nodesUpdated(0)))
          mockZooKeeper.getChildren(availabilityNode, true) returns (availability) thenReturns (availabilityUpdated)
          mockZooKeeper.getData("%s/%d".format(membershipNode, 2), clusterManager.getWatcher, null) returns (Node.nodeToByteArray(nodesUpdated(1)))

          clusterManager ! Connected

          connectedCount must eventually(be_==(1))
          nodesReceived.size must be_==(1)

          clusterManager ! NodeChildrenChanged(membershipNode)

          nodesReceived.size must eventually(be_==(2))
          nodesReceived must containAll(nodesUpdated)
          nodesReceived.foreach { node => node must be_==(nodesUpdated(node.id - 1)) }
        }
      }

      "and the availability node changed" in {
        "update the node availability and notify listeners" in {
          val membership = new ArrayList[String]
          membership.add("1")
          membership.add("2")
          membership.add("3")

          val availability = new ArrayList[String]
          availability.add("2")

          val newAvailability = new ArrayList[String]
          newAvailability.add("1")
          newAvailability.add("3")

          val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
            Node(2, "localhost:31314", true, Set(2, 3)), Node(3, "localhost:31315", false, Set(2, 3)))

          mockZooKeeper.getChildren(membershipNode, true) returns membership
          nodes.foreach { node =>
            mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), clusterManager.getWatcher, null) returns Node.nodeToByteArray(node)
          }
          mockZooKeeper.getChildren(availabilityNode, true) returns availability thenReturns newAvailability

          clusterManager ! Connected

          nodesReceived.size must eventually(be_==(3))
          nodesReceived must containAll(nodes)
          nodesReceived.foreach { n =>
            if (n.id == 2) n.available must beTrue else n.available must beFalse
          }

          clusterManager ! NodeChildrenChanged(availabilityNode)

          nodesChangedCount must eventually(be_==(1))
          nodesReceived.size must be_==(3)
          nodesReceived must containAll(nodes)
          nodesReceived.foreach { n =>
            if (n.id == 2) n.available must beFalse else n.available must beTrue
          }

          there were two(mockZooKeeper).getChildren(availabilityNode, true)
        }

        "handle the case that all nodes are unavailable" in {
          val membership = new ArrayList[String]
          membership.add("1")
          membership.add("2")
          membership.add("3")

          val newAvailability = new ArrayList[String]

          val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
            Node(2, "localhost:31314", true, Set(2, 3)), Node(3, "localhost:31315", false, Set(2, 3)))

          mockZooKeeper.getChildren(membershipNode, true) returns membership
          nodes.foreach { node =>
            mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), clusterManager.getWatcher, null) returns Node.nodeToByteArray(node)
          }
          mockZooKeeper.getChildren(availabilityNode, true) returns membership thenReturns newAvailability

          clusterManager ! Connected

          nodesReceived.size must eventually(be_==(3))
          nodesReceived must containAll(nodes)
          nodesReceived.foreach { _.available must beTrue }

          clusterManager ! NodeChildrenChanged(availabilityNode)

          nodesChangedCount must eventually(be_==(1))
          nodesReceived.size must be_==(3)
          nodesReceived must containAll(nodes)
          nodesReceived.foreach { n => n.available must beFalse }

          there were two(mockZooKeeper).getChildren(availabilityNode, true)
        }

        "do nothing if not connected" in {
          clusterManager ! NodeChildrenChanged(availabilityNode)

          nodesChangedCount must eventually(be_==(0))
        }
      }

        "update the nodes and notify listeners" in {
          val membership = new ArrayList[String]
          membership.add("1")
          membership.add("2")

          val newMembership = new ArrayList[String]
          newMembership.add("1")
          newMembership.add("2")
          newMembership.add("3")

          val updatedNodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
            Node(2, "localhost:31314", true, Set(2, 3)), Node(3, "localhost:31315", false, Set(2, 3)))
          val nodes = updatedNodes.slice(0, 2)

          mockZooKeeper.getChildren(membershipNode, true) returns membership thenReturns newMembership
          updatedNodes.foreach { node =>
            mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), clusterManager.getWatcher, null) returns Node.nodeToByteArray(node)
          }
          mockZooKeeper.getChildren(availabilityNode, true) returns membership

          clusterManager ! Connected

          nodesReceived.size must eventually(be_==(2))
          nodesReceived must containAll(nodes)

          clusterManager ! NodeChildrenChanged(membershipNode)

          nodesChangedCount must eventually(be_==(1))
          nodesReceived.size must be_==(3)
          nodesReceived must containAll(updatedNodes)

          got {
            two(mockZooKeeper).getChildren(availabilityNode, true)
            two(mockZooKeeper).getChildren(membershipNode, true)
          }
        }

        "handle the case that a node is removed" in {
          val membership = new ArrayList[String]
          membership.add("1")
          membership.add("2")
          membership.add("3")

          val newMembership = new ArrayList[String]
          newMembership.add("1")
          newMembership.add("3")

          val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
            Node(2, "localhost:31314", true, Set(2, 3)), Node(3, "localhost:31315", false, Set(2, 3)))

          mockZooKeeper.getChildren(membershipNode, true) returns membership thenReturns newMembership
          nodes.foreach { node =>
            mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), clusterManager.getWatcher, null) returns Node.nodeToByteArray(node)
          }
          mockZooKeeper.getChildren(availabilityNode, true) returns membership

          clusterManager ! Connected

          nodesReceived.size must eventually(be_==(3))
          nodesReceived must containAll(nodes)
          nodesReceived.foreach { _.available must beTrue }

          clusterManager ! NodeChildrenChanged(membershipNode)

          nodesChangedCount must eventually(be_==(1))
          nodesReceived.size must be_==(2)
          nodesReceived must containAll(List(nodes(0), nodes(2)))

          there were two(mockZooKeeper).getChildren(membershipNode, true)
        }

        "handle the case that a node is removed" in {
          val membership = new ArrayList[String]
          membership.add("1")
          membership.add("2")
          membership.add("3")

          val newMembership = new ArrayList[String]
          newMembership.add("1")
          newMembership.add("3")

          val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
            Node(2, "localhost:31314", true, Set(2, 3)), Node(3, "localhost:31315", false, Set(2, 3)))

          mockZooKeeper.getChildren(membershipNode, true) returns membership thenReturns newMembership
          nodes.foreach { node =>
            mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), clusterManager.getWatcher, null) returns Node.nodeToByteArray(node)
          }
          mockZooKeeper.getChildren(availabilityNode, true) returns membership

          clusterManager ! Connected

          nodesReceived.size must eventually(be_==(3))
          nodesReceived must containAll(nodes)
          nodesReceived.foreach { _.available must beTrue }

          clusterManager ! NodeChildrenChanged(membershipNode)

          nodesChangedCount must eventually(be_==(1))
          nodesReceived.size must be_==(2)
          nodesReceived must containAll(List(nodes(0), nodes(2)))

          there were two(mockZooKeeper).getChildren(membershipNode, true)
        }

        "do nothing if not connected" in {
          clusterManager ! NodeChildrenChanged(membershipNode)

          nodesChangedCount must eventually(be_==(0))
        }
      }

    "when a Shutdown message is received" in {
      "shop handling events" in {
        doNothing.when(mockZooKeeper).close
        var callCount = 0
        def countedZkf(connectString: String, sessionTimeout: Int, watcher: Watcher) = {
          callCount += 1
          mockZooKeeper
        }

        val zkm = new ZooKeeperClusterManager("", 0, "")(countedZkf _)
        zkm.start
        clusterManager ! Shutdown
        clusterManager ! Connected

        waitFor(10.ms)
        callCount must eventually(be_==(1))
        there was one(mockZooKeeper).close

        zkm ! Shutdown
      }
    }

    "when a AddNode message is received" in {
      val node = Node(1, "localhost:31313", false, Set(1, 2))

      "throw a ClusterDisconnectedException if not connected" in {

        clusterManager !? AddNode(node) match {
          case ClusterManagerResponse(r) => r must beSome[ClusterException].which(_ must haveClass[ClusterDisconnectedException])
        }
      }

      "throw an InvalidNodeException if the node already exists" in {
        val path = membershipNode + "/1"
        mockZooKeeper.exists(path, false) returns mock[Stat]

        clusterManager ! Connected
        clusterManager !? AddNode(node) match {
          case ClusterManagerResponse(r) => r must beSome[ClusterException].which(_ must haveClass[InvalidNodeException])
        }

        there was one(mockZooKeeper).exists(path, false)
      }

      "add the node to ZooKeeper" in {
        val path = membershipNode + "/1"
        mockZooKeeper.exists(path, false) returns null
        mockZooKeeper.create(path, node, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) returns path

        clusterManager ! Connected
        clusterManager !? AddNode(node) match {
          case ClusterManagerResponse(r) => r must beNone
        }

        got {
          one(mockZooKeeper).exists(path, false)
          one(mockZooKeeper).create(path, node, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        }
      }

      "notify listeners that the node list changed" in {
        clusterManager ! Connected
        clusterManager !? AddNode(node)

        nodesChangedCount must eventually(be_==(1))
        nodesReceived.size must be_==(1)
        nodesReceived must contain(node)
      }
    }

    "when a RemoveNode message is received" in {
      "throw a ClusterDisconnectedException if not connected" in {
        clusterManager !? RemoveNode(1) match {
          case ClusterManagerResponse(r) => r must beSome[ClusterException].which(_ must haveClass[ClusterDisconnectedException])
        }
      }

      "do nothing if the node does not exist in ZooKeeper" in {
        mockZooKeeper.exists(membershipNode + "/1", false) returns null

        clusterManager ! Connected
        clusterManager !? RemoveNode(1) match {
          case ClusterManagerResponse(r) => r must beNone
        }

        there was one(mockZooKeeper).exists(membershipNode + "/1", false)
      }

      "remove the znode from ZooKeeper if the node exists" in {
        val path = membershipNode + "/1"

        mockZooKeeper.exists(path, false) returns mock[Stat]
        doNothing.when(mockZooKeeper).delete(path, -1)

        clusterManager ! Connected
        clusterManager !? RemoveNode(1) match {
          case ClusterManagerResponse(r) => r must beNone
        }

        there was one(mockZooKeeper).delete(path, -1)
      }

      "notify listeners that the node list changed" in {
        val membership = new ArrayList[String]
        membership.add("1")
        membership.add("2")
        membership.add("3")

        val availability = membership.clone.asInstanceOf[ArrayList[String]]
        availability.remove(2)

        val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
          Node(2, "localhost:31314", false, Set(2, 3)), Node(3, "localhost:31315", true, Set(2, 3)))

        mockZooKeeper.getChildren(membershipNode, true) returns membership
        nodes.foreach { node =>
          mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), clusterManager.getWatcher, null) returns Node.nodeToByteArray(node)
        }
        mockZooKeeper.getChildren(availabilityNode, true) returns availability

        clusterManager ! Connected
        clusterManager !? RemoveNode(2)

        nodesReceived.size must eventually(be_==(2))
        nodesReceived must containAll(Array(nodes(0), nodes(2)))
      }
    }

    "when a MarkNodeAvailable message is received" in {
      "throw a ClusterDisconnectedException if not connected" in {
        clusterManager !? MarkNodeAvailable(1) match {
          case ClusterManagerResponse(r) => r must beSome[ClusterException].which(_ must haveClass[ClusterDisconnectedException])
        }
      }

      "add the znode to ZooKeeper if it doesn't exist" in {
        val znodes = List(rootNode, membershipNode, availabilityNode)
        znodes.foreach(mockZooKeeper.exists(_, false) returns mock[Stat])

        val path = availabilityNode + "/1"

        mockZooKeeper.exists(path, false) returns null
        mockZooKeeper.create(path, Array[Byte](0), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) returns path

        clusterManager ! Connected
        clusterManager !? MarkNodeAvailable(1) match {
          case ClusterManagerResponse(r) => r must beNone
        }

        got {
          one(mockZooKeeper).exists(path, false)
          one(mockZooKeeper).create(path, Array[Byte](0), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        }
      }

      "do nothing if the znode already exists" in {
        val znodes = List(rootNode, membershipNode, availabilityNode)
        znodes.foreach(mockZooKeeper.exists(_, false) returns mock[Stat])

        val path = availabilityNode + "/1"

        mockZooKeeper.exists(path, false) returns mock[Stat]
        mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) returns path

        clusterManager ! Connected
        clusterManager !? MarkNodeAvailable(1) match {
          case ClusterManagerResponse(r) => r must beNone
        }

        there was one(mockZooKeeper).exists(path, false)
        there was no(mockZooKeeper).create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      }

      "notify listeners that the node list changed" in {
        val membership = new ArrayList[String]
        membership.add("1")
        membership.add("2")
        membership.add("3")

        val availability = membership.clone.asInstanceOf[ArrayList[String]]
        availability.remove(2)

        val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
          Node(2, "localhost:31314", false, Set(2, 3)), Node(3, "localhost:31315", true, Set(2, 3)))

        mockZooKeeper.getChildren(membershipNode, true) returns membership
        nodes.foreach { node =>
          mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), clusterManager.getWatcher, null) returns Node.nodeToByteArray(node)
        }
        mockZooKeeper.getChildren(availabilityNode, true) returns availability

        clusterManager ! Connected

        nodesReceived.size must eventually(be_>(0))
        nodesReceived.foreach { node =>
          if (node.id == 3) node.available must beFalse
        }

        clusterManager !? MarkNodeAvailable(3)
        waitFor(10.ms)

        nodesReceived.foreach { node =>
          if (node.id == 3) node.available must beTrue
        }
      }
    }

    "when a MarkNodeUnavailable message is received" in {
      "throw a ClusterDisconnectedException if not connected" in {
        clusterManager !? MarkNodeUnavailable(1) match {
          case ClusterManagerResponse(r) => r must beSome[ClusterException].which(_ must haveClass[ClusterDisconnectedException])
        }
      }

      "do nothing if the node does not exist in ZooKeeper" in {
        mockZooKeeper.exists(availabilityNode + "/1", false) returns mock[Stat]

        clusterManager ! Connected
        clusterManager !? MarkNodeUnavailable(1) match {
          case ClusterManagerResponse(r) => r must beNone
        }

        there was one(mockZooKeeper).exists(availabilityNode + "/1", false)
      }

      "remove the znode from ZooKeeper if the node exists" in {
        val path = availabilityNode + "/1"

        mockZooKeeper.exists(path, false) returns mock[Stat]
        doNothing.when(mockZooKeeper).delete(path, -1)

        clusterManager ! Connected
        clusterManager !? MarkNodeUnavailable(1) match {
          case ClusterManagerResponse(r) => r must beNone
        }

        there was one(mockZooKeeper).delete(path, -1)
      }

      "notify listeners that the node list changed" in {
        val membership = new ArrayList[String]
        membership.add("1")
        membership.add("2")
        membership.add("3")

        val availability = membership.clone.asInstanceOf[ArrayList[String]]
        availability.remove(2)

        val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
          Node(2, "localhost:31314", false, Set(2, 3)), Node(3, "localhost:31315", true, Set(2, 3)))

        mockZooKeeper.getChildren(membershipNode, true) returns membership
        nodes.foreach { node =>
          mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), clusterManager.getWatcher, null) returns Node.nodeToByteArray(node)
        }
        mockZooKeeper.getChildren(availabilityNode, true) returns availability

        clusterManager ! Connected

        nodesReceived.size must eventually(be_>(0))
        nodesReceived.foreach { node =>
          if (node.id == 1) node.available must beTrue
        }

        clusterManager !? MarkNodeUnavailable(1)
        waitFor(10.ms)

        nodesReceived.foreach { node =>
          if (node.id == 1) node.available must beFalse
        }
      }
    }

    "when a SetNodeCapability message is received" in {
      "throw a ClusterDisconnectedException if not connected" in {
        clusterManager !?  SetNodeCapability(1, 1L) match {
          case ClusterManagerResponse(r) => r must beSome[ClusterException].which(_ must haveClass[ClusterDisconnectedException])
        }
      }

      "markNodeAvailable will set node to initialCapability" in {
        val znodes = List(rootNode, membershipNode, availabilityNode)
        znodes.foreach(mockZooKeeper.exists(_, false) returns mock[Stat])

        val path = availabilityNode + "/1"

        mockZooKeeper.exists(path, false) returns null
        mockZooKeeper.create(path, Array[Byte](59, 30), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) returns path

        clusterManager ! Connected
        clusterManager !? MarkNodeAvailable(1, 15134L) match {
          case ClusterManagerResponse(r) => r must beNone
        }

        got {
          one(mockZooKeeper).exists(path, false)
          one(mockZooKeeper).create(path, Array[Byte](59,30), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        }
      }

      "do nothing if the node is not available yet in Zookeeper" in {
        mockZooKeeper.exists(availabilityNode + "/1", false) returns null
        mockZooKeeper.setData(availabilityNode + "/1", Array[Byte](0), -1) returns mock[Stat]

        clusterManager ! Connected
        clusterManager !? SetNodeCapability(1, 1L) match {
          case ClusterManagerResponse(r) => r must beNone
        }

        there was one(mockZooKeeper).exists(availabilityNode + "/1", false)
        there was no(mockZooKeeper).setData(availabilityNode + "/1", Array[Byte](0), -1)
      }

      "when we initialize data we read in the permanent capabilities correctly from zookeeper" in {
        val membership = new ArrayList[String]
        membership.add("1")

        val availability = membership.clone.asInstanceOf[ArrayList[String]]
        val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2), Some(0L), Some(5L)))

        mockZooKeeper.getChildren(membershipNode, true) returns membership
        nodes.foreach { node =>
          mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), clusterManager.getWatcher, null) returns Node.nodeToByteArray(node)
        }
        mockZooKeeper.getChildren(availabilityNode, true) returns availability

        clusterManager ! Connected

        connectedCount must eventually(be_==(1))
        nodesReceived.size must be_==(1)
        nodesReceived must containAll(nodes)
        nodesReceived.foreach { node => node must be_==(nodes(node.id - 1)) }
      }

      "change permanent capability in zookeeper when node is not part of the members" in {
        val membership = new ArrayList[String]
        membership.add("1")
        membership.add("2")
        membership.add("3")

        val availability = membership.clone.asInstanceOf[ArrayList[String]]

        val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
          Node(2, "localhost:31314", true, Set(2, 3)), Node(3, "localhost:31315", true, Set(2, 3)))

        mockZooKeeper.getChildren(membershipNode, true) returns membership
        nodes.foreach { node =>
          //zk.getData("%s/%s".format(MEMBERSHIP_NODE, memberId),watcher,null)
          mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), clusterManager.getWatcher, null) returns Node.nodeToByteArray(node)
        }
        mockZooKeeper.getChildren(availabilityNode, true) returns availability
        mockZooKeeper.getData("%s/%d".format(membershipNode, 4), clusterManager.getWatcher, null)

        clusterManager ! Connected
        clusterManager ! NodeDataChanged("%s/%d".format(membershipNode, 4))
        waitFor(50.ms)

        //make sure nothing is changed in terms of the nodes
        connectedCount must eventually(be_==(1))
        nodesReceived.size must be_==(3)
        nodesReceived must containAll(nodes)
        nodesReceived.foreach { node => node must be_==(nodes(node.id - 1)) }
      }

      "change permanent capability in zookeeper when node is part of the members" in {
        val membership = new ArrayList[String]
        membership.add("1")
        membership.add("2")
        membership.add("3")

        val availability = membership.clone.asInstanceOf[ArrayList[String]]

        val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
          Node(2, "localhost:31314", true, Set(2, 3)), Node(3, "localhost:31315", true, Set(2, 3)))
        val nodeUpdated = Node(3, "localhost:31314", true, Set(2, 3), Some(0L), Some(7L))

        mockZooKeeper.getChildren(membershipNode, true) returns membership
        mockZooKeeper.getData("%s/%d".format(membershipNode, 1), clusterManager.getWatcher, null) returns Node.nodeToByteArray(nodes(0))
        mockZooKeeper.getData("%s/%d".format(membershipNode, 2), clusterManager.getWatcher, null) returns Node.nodeToByteArray(nodes(1))
        mockZooKeeper.getData("%s/%d".format(membershipNode, 3), clusterManager.getWatcher, null) returns Node.nodeToByteArray(nodes(2)) thenReturns nodeUpdated
        mockZooKeeper.getChildren(availabilityNode, true) returns availability

        clusterManager ! Connected
        connectedCount must eventually(be_==(1))
        clusterManager ! NodeDataChanged("%s/%d".format(membershipNode, 3))
        waitFor(50.ms)

        //make sure that getData was called twice
        got {
          two(mockZooKeeper).getData("%s/%d".format(membershipNode, 3), clusterManager.getWatcher, null)
          one(mockZooKeeper).getData("%s/%d".format(membershipNode, 2), clusterManager.getWatcher, null)
          one(mockZooKeeper).getData("%s/%d".format(membershipNode, 1), clusterManager.getWatcher, null)
        }
        nodes(2) = nodeUpdated
        nodesReceived.size must be_==(3)
        nodesReceived must containAll(nodes)
        nodesReceived.foreach { node => node must be_==(nodes(node.id - 1)) }
      }

      "change capability data in Zookeeper when node is available" in{
        mockZooKeeper.exists(availabilityNode + "/1", false) returns mock[Stat]
        mockZooKeeper.setData(availabilityNode + "/1", Array[Byte](1), -1) returns mock[Stat]
        clusterManager ! Connected
        clusterManager ! MarkNodeAvailable(1)
        clusterManager !? SetNodeCapability(1, 1L) match {
          case ClusterManagerResponse(r) => r must beNone
        }
        there was two(mockZooKeeper).exists(availabilityNode + "/1", false)
        there was one(mockZooKeeper).setData(availabilityNode + "/1", Array[Byte](1), -1)
      }

      "notify listeners that capablity has changed" in {
        val membership = new ArrayList[String]
        membership.add("1")
        membership.add("2")
        membership.add("3")

        val availability = membership.clone.asInstanceOf[ArrayList[String]]
        availability.remove(2)

        val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
                          Node(2, "localhost:31314", false, Set(2, 3)), Node(3, "localhost:31315", true, Set(2, 3)))

        mockZooKeeper.getChildren(membershipNode, true) returns membership
        nodes.foreach { node =>
          mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), clusterManager.getWatcher, null) returns Node.nodeToByteArray(node)
                      }
        mockZooKeeper.getChildren(availabilityNode, true) returns availability

        clusterManager ! Connected

        nodesReceived.size must eventually(be_>(0))
        nodesReceived.foreach { node =>
          if (node.id == 3) node.available must beFalse
        }

        clusterManager !? MarkNodeAvailable(3)
        waitFor(10.ms)

        nodesReceived.foreach { node =>
          if (node.id == 3)
          {
            node.available must beTrue
            node.capability.isDefined must beTrue
            node.capability.get must be_==(0L)
          }
        }

        clusterManager !? SetNodeCapability(3, 3L)
        waitFor(10.ms)
        nodesReceived.filter { node => (node.id == 3) }.foreach {
          node =>
            node.available must beTrue
            node.capability.isDefined must beTrue
            node.capability.get must be_==(3L)
        }
      }

      "when membership changes current nodes will be re-calculated" in {
        val membership = new ArrayList[String]
        membership.add("1")
        membership.add("2")

        val availability = membership.clone.asInstanceOf[ArrayList[String]]
        val nodes = Array(Node(1, "localhost:31313", true, Set(1,2)), Node(2, "localhost:31314", true, Set(2,3)))

        mockZooKeeper.getChildren(membershipNode, true) returns membership
        nodes.foreach { node  =>
          mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), clusterManager.getWatcher, null) returns Node.nodeToByteArray(node)
                      }
        mockZooKeeper.getChildren(availabilityNode, true) returns availability
        mockZooKeeper.getData(availabilityNode + "/1", false, null) returns null
        mockZooKeeper.getData(availabilityNode + "/2", false, null) returns Array[Byte](3,2,4)
        mockZooKeeper.getData(availabilityNode + "/3", false, null) returns Array[Byte](0)
        mockZooKeeper.getData(membershipNode + "/3", clusterManager.getWatcher, null) returns Node.nodeToByteArray(Node(3, "localhost:31315", false, Set(3,4)))

        clusterManager ! Connected
        nodesReceived.size must eventually(be_==(2))

        membership.add("3")
        mockZooKeeper.getChildren(membershipNode, true) returns membership

        clusterManager ! NodeChildrenChanged(membershipNode)
        nodesChangedCount must eventually(be_==(1))

        nodesReceived.find((node) => node.id == 1).get.capability.get must be_==(0L)
        nodesReceived.find((node) => node.id == 2).get.capability.get must be_==(197124L)
        nodesReceived.find((node) => node.id == 3).get.capability mustEq None

        got {
          two(mockZooKeeper).getData(availabilityNode + "/1", false, null)
          two(mockZooKeeper).getData(availabilityNode + "/2", false, null)
          no(mockZooKeeper).getData(availabilityNode + "/3", false, null)
        }
      }

    }
    doAfterSpec {
      actors.Scheduler.shutdown
    }
  }

  "ClusterWatcher" should {
    import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}

    var connectedCount = 0
    var disconnectedCount = 0
    var expiredCount = 0
    var nodesChangedCount = 0
    var nodesChangedPath = ""

    val zkm = actor {
      react {
        case Connected => connectedCount += 1
        case Disconnected => disconnectedCount += 1
        case Expired => expiredCount += 1
        case NodeChildrenChanged(path) => nodesChangedCount += 1; nodesChangedPath = path
      }
    }

    val clusterWatcher = new ClusterWatcher(zkm)

    def newEvent(state: KeeperState) = {
      val event = mock[WatchedEvent]
      event.getType returns EventType.None
      event.getState returns state

      event
    }

    "send a Connected event when ZooKeeper connects" in {
      val event = newEvent(KeeperState.SyncConnected)

      clusterWatcher.process(event)

      connectedCount must eventually(be_==(1))
    }

    "send a Disconnected event when ZooKeeper disconnects" in {
      val event = newEvent(KeeperState.Disconnected)

      clusterWatcher.process(event)

      disconnectedCount must eventually(be_==(1))
    }

    "send an Expired event when ZooKeeper's connection expires" in {
      val event = newEvent(KeeperState.Expired)

      clusterWatcher.process(event)

      expiredCount must eventually(be_==(1))
    }

    "send a NodeChildrenChanged event when nodes change" in {
      val event = mock[WatchedEvent]
      event.getType returns EventType.NodeChildrenChanged
      val path = "thisIsThePath"
      event.getPath returns path

      clusterWatcher.process(event)

      nodesChangedCount must eventually(be_==(1))
      nodesChangedPath must be_==(path)
    }

    doAfterSpec {
      actors.Scheduler.shutdown
    }
  }
}
