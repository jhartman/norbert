package com.linkedin.norbert.javacompat.network;


import com.linkedin.norbert.javacompat.cluster.Node;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;


public class ConsistentHashPartitionedLoadBalancer<PartitionedId> implements PartitionedLoadBalancer<PartitionedId>
{
  private final HashFunction<String> _hashFunction;
  private final TreeMap<Long, Map<Node, Set<Integer>>> _routingMap;

  ConsistentHashPartitionedLoadBalancer(int bucketCount, HashFunction<String> hashFunction, Set<Endpoint> endpoints)
  {
    _hashFunction = hashFunction;
    _routingMap = new TreeMap<Long, Map<Node, Set<Integer>>>();

    // Gather set of nodes for each partition
    Map<Integer, Set<Node>> partitionNodes = new TreeMap<Integer, Set<Node>>();
    for (Endpoint endpoint : endpoints)
    {
      Node node = endpoint.getNode();
      for (Integer partId : node.getPartitionIds())
      {
        Set<Node> partNodes = partitionNodes.get(partId);
        if (partNodes == null)
        {
          partNodes = new HashSet<Node>();
          partitionNodes.put(partId, partNodes);
        }
        partNodes.add(node);
      }
    }

    // Builds individual ring for each partitions
    int maxSize = 0;
    Map<Integer, NavigableMap<Long, Node>> rings = new TreeMap<Integer, NavigableMap<Long, Node>>();
    for (Map.Entry<Integer, Set<Node>> entry : partitionNodes.entrySet())
    {
      Integer partId = entry.getKey();
      NavigableMap<Long, Node> ring = rings.get(partId);
      if (ring == null)
      {
        ring = new TreeMap<Long, Node>();
        rings.put(partId, ring);
      }

      if (maxSize < entry.getValue().size())
      {
        maxSize = entry.getValue().size();
      }

      for (Node node : entry.getValue())
      {
        for (int i = 0; i < bucketCount; i++)
        {
          // Use node-[node_id]-[bucket_id] as key
          // Hence for the same node, same bucket id will always hash to the same place
          // This helps to maintain consistency when the bucketCount changed
          ring.put(hashFunction.hash(String.format("node-%d-%d", node.getId(), i)), node);
        }
      }
    }

    // Build one final ring.
    for (int slot = 0; slot < bucketCount * maxSize; slot++)
    {
      Long point = hashFunction.hash(String.format("ring-%d", slot));

      // For each generated point on the ring, gather node for each partition.
      Map<Node, Set<Integer>> pointRoute = new HashMap<Node, Set<Integer>>();
      for (Map.Entry<Integer, NavigableMap<Long, Node>> ringEntry : rings.entrySet())
      {
        Node node = lookup(ringEntry.getValue(), point);

        Set<Integer> partitionSet = pointRoute.get(node);
        if (partitionSet == null)
        {
          partitionSet = new HashSet<Integer>();
          pointRoute.put(node, partitionSet);
        }
        partitionSet.add(ringEntry.getKey()); // Add partition to the node
      }
      _routingMap.put(point, pointRoute);
    }
  }

  @Override
  public Node nextNode(PartitionedId partitioneId)
  {
    // TODO: How do we choose which node to return if we don't want to throw Exception?
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<Node, Set<Integer>> nodesForOneReplica(PartitionedId partitioneId)
  {
    return lookup(_routingMap, _hashFunction.hash(partitioneId.toString()));
  }

  private <K, V> V lookup(NavigableMap<K, V> ring, K key)
  {
    V result = ring.get(key);
    if (result == null)
    {       // Not a direct match
      Map.Entry<K, V> entry = ring.ceilingEntry(key);
      result = (entry == null) ? ring.firstEntry().getValue() : entry.getValue();
    }

    return result;
  }
}
