package com.linkedin.norbert.javacompat.network

/**
 *
 * @author Dmytro Ivchenko
 */
class PartitionedNetworkClientFactory[PartitionedId](serviceName: String, zooKeeperConnectString: String,
                                                     zooKeeperSessionTimeoutMillis: Int,
                                                     closeChannelTimeMillis: Long, norbertOutlierMultiplier: Double, norbertOutlierConstant: Double,
                                                     partitionedLoadBalancerFactory: PartitionedLoadBalancerFactory[PartitionedId])
{
  val config = new NetworkClientConfig

  config.setServiceName(serviceName);
  config.setZooKeeperConnectString(zooKeeperConnectString);
  config.setZooKeeperSessionTimeoutMillis(zooKeeperSessionTimeoutMillis);
  config.setCloseChannelTimeMillis(closeChannelTimeMillis);
  config.setOutlierMuliplier(norbertOutlierMultiplier);
  config.setOutlierConstant(norbertOutlierConstant);

  def createPartitionedNetworkClient(): PartitionedNetworkClient[PartitionedId] =
  {
    new NettyPartitionedNetworkClient[PartitionedId](config, partitionedLoadBalancerFactory);
  }
}
