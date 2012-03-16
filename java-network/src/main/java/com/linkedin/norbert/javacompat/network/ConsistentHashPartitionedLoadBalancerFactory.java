package com.linkedin.norbert.javacompat.network;


import java.util.HashSet;
import java.util.Set;


public class ConsistentHashPartitionedLoadBalancerFactory<PartitionedId> implements PartitionedLoadBalancerFactory<PartitionedId>
{
  private final int _bucketCount;
  private final HashFunction<String> _hashFunction;

  public ConsistentHashPartitionedLoadBalancerFactory(int bucketCount)
  {
    this(bucketCount, new HashFunction.MD5HashFunction());
  }

  public ConsistentHashPartitionedLoadBalancerFactory(int bucketCount, HashFunction<String> hashFunction)
  {
    _bucketCount = bucketCount;
    _hashFunction = hashFunction;
  }

  @Override
  public PartitionedLoadBalancer<PartitionedId> newLoadBalancer(Set<Endpoint> endpoints)
      throws InvalidClusterException
  {
    return new ConsistentHashPartitionedLoadBalancer<PartitionedId>(_bucketCount, _hashFunction, endpoints);
  }

  @Override
  public Integer getNumPartitions(Set<Endpoint> endpoints)
  {
    Set<Integer> set = new HashSet<Integer>();
    for (Endpoint endpoint : endpoints)
    {
      set.addAll(endpoint.getNode().getPartitionIds());
    }

    return set.size();
  }
}
