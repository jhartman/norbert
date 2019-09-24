package com.linkedin.norbert.javacompat.network


import java.util
import com.linkedin.norbert.EndpointConversions
import com.linkedin.norbert.javacompat.cluster.{JavaNode, Node}


/**
 *
 * @author Dmytro Ivchenko
 */
class RoundRobinLoadBalancerFactory extends LoadBalancerFactory
{
  val scalaLbf = new com.linkedin.norbert.network.client.loadbalancer.RoundRobinLoadBalancerFactory

  def newLoadBalancer(endpoints: util.Set[Endpoint]): LoadBalancer = {
    val loadBalancer = scalaLbf.newLoadBalancer(EndpointConversions.convertJavaEndpointSet(endpoints))

    new LoadBalancer {
      def nextNode: Node = {
        nextNode(0L, 0L)
      }

      def nextNode(capability: java.lang.Long): Node = {
        nextNode(capability, 0L)
      }

      def nextNode(capability: java.lang.Long, persistentCapability: java.lang.Long): Node = {
        val node = loadBalancer.nextNode(new Some(capability.longValue()), new Some(persistentCapability.longValue()))
        if (node.isDefined)
          JavaNode.apply(node.get)
        else
          null
      }
    }
  }
}
