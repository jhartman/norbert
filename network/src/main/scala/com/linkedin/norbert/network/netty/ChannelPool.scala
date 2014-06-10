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
package network
package netty

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.group.{ChannelGroup, DefaultChannelGroup}
import org.jboss.netty.channel.{ChannelFutureListener, ChannelFuture, Channel}
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder
import java.util.concurrent.{TimeoutException, ArrayBlockingQueue, LinkedBlockingQueue}
import java.net.InetSocketAddress
import jmx.JMX.MBean
import jmx.JMX
import logging.Logging
import cluster.{Node, ClusterClient}
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean, AtomicInteger}
import norbertutils.{Clock, SystemClock}
import java.io.IOException
import common.{BackoffStrategy, SimpleBackoffStrategy}
import java.util

class ChannelPoolClosedException extends Exception

class ChannelPoolFactory(maxConnections: Int, openTimeoutMillis: Int, writeTimeoutMillis: Int,
                         bootstrap: ClientBootstrap,
                         errorStrategy: Option[BackoffStrategy],
                         closeChannelTimeMillis: Long) {

  def newChannelPool(address: InetSocketAddress): ChannelPool = {
    val group = new DefaultChannelGroup("norbert-client [%s]".format(address))
    new ChannelPool(address = address,
      maxConnections = maxConnections,
      openTimeoutMillis = openTimeoutMillis,
      writeTimeoutMillis = writeTimeoutMillis,
      bootstrap = bootstrap,
      channelGroup = group,
      closeChannelTimeMillis = closeChannelTimeMillis,
      errorStrategy = errorStrategy,
      clock = SystemClock)
  }

  def shutdown: Unit = {
    bootstrap.releaseExternalResources
  }
}

class ChannelPool(address: InetSocketAddress, maxConnections: Int, openTimeoutMillis: Int, writeTimeoutMillis: Int,
                  bootstrap: ClientBootstrap,
                  channelGroup: ChannelGroup,
                  closeChannelTimeMillis: Long,
                  val errorStrategy: Option[BackoffStrategy],
                  clock: Clock) extends Logging {

  case class PoolEntry(channel: Channel, creationTime: Long) {
    def age = System.currentTimeMillis() - creationTime

    def reuseChannel(closeChannelTimeMillis: Long) = closeChannelTimeMillis < 0 || age < closeChannelTimeMillis
  }

  private val pool = new ArrayBlockingQueue[PoolEntry](maxConnections)
  private val waitingWrites = new LinkedBlockingQueue[Request[_, _]]
  private val poolSize = new AtomicInteger(0)
  private val closed = new AtomicBoolean
  private val softClosed = new AtomicBoolean
  private val requestsSent = new AtomicInteger(0)
  private val lock = new java.util.concurrent.locks.ReentrantReadWriteLock(true)

  private val jmxHandle = JMX.register(new MBean(classOf[ChannelPoolMBean], "address=%s,port=%d".format(address.getHostName, address.getPort)) with ChannelPoolMBean {
    import scala.math._
    def getWriteQueueSize = waitingWrites.size

    def getOpenChannels = poolSize.get

    def getMaxChannels = maxConnections

    def getNumberRequestsSent = requestsSent.get.abs
  })

  def sendRequest[RequestMsg, ResponseMsg](request: Request[RequestMsg, ResponseMsg]): Unit = if (closed.get) {
    throw new ChannelPoolClosedException
  } else {
    checkoutChannel match {
      case Some(poolEntry) =>
        writeRequestToChannel(request, poolEntry.channel)
        checkinChannel(poolEntry)

      case None =>
        waitingWrites.offer(request)
        openChannel(request)
    }
  }
 
  /*
   * This is a soft close where we get rid of the JMX values only.
   * The reason for splitting it in two phases is to avoid extra synchronization overhead
  */ 
  def unregisterJMX {
    softClosed.set(true)
    jmxHandle.foreach {JMX.unregister(_)}
  }

  def close {
    if (closed.compareAndSet(false, true)) {
      if(!softClosed.get()) {
        jmxHandle.foreach {JMX.unregister(_)}
      }
      channelGroup.close.awaitUninterruptibly
    }
  }

  private def checkinChannel(poolEntry: PoolEntry, isFirstWriteToChannel: Boolean = false) {
    while (!waitingWrites.isEmpty) {
      waitingWrites.poll match {
        case null => // do nothing

        case request =>
          val timeout = if (isFirstWriteToChannel) writeTimeoutMillis + openTimeoutMillis else writeTimeoutMillis
          if((System.currentTimeMillis - request.timestamp) < timeout)
            writeRequestToChannel(request, poolEntry.channel)
          else
            request.onFailure(new TimeoutException("Timed out while waiting to write"))
      }
    }

    if(poolEntry.reuseChannel(closeChannelTimeMillis))
      pool.offer(poolEntry)
    else {
      poolSize.decrementAndGet()
      poolEntry.channel.close()
    }
  }

  private def checkoutChannel: Option[PoolEntry] = {
    var poolEntry: PoolEntry = null
    var found = false
    while (!pool.isEmpty && !found) {
      pool.poll match {
        case null => // do nothing

        case pe =>
          if (pe.channel.isConnected) {
            if(pe.reuseChannel(closeChannelTimeMillis)) {
              poolEntry = pe
              found = true
            } else {
              poolSize.decrementAndGet()
              pe.channel.close()
            }
          } else {
            poolSize.decrementAndGet()
          }
      }
    }

    Option(poolEntry)
  }

  private def openChannel(request: Request[_, _]) {
    if (poolSize.incrementAndGet > maxConnections) {
      poolSize.decrementAndGet
      log.warn("Unable to open channel, pool is full. Waiting for another channel to return to queue before processing")
    } else {
      log.debug("Opening a channel to: %s".format(address))

      bootstrap.connect(address).addListener(new ChannelFutureListener {
        def operationComplete(openFuture: ChannelFuture) = {
          if (openFuture.isSuccess) {
            val channel = openFuture.getChannel
            log.debug("Opened a channel to: %s".format(address))

            channelGroup.add(channel)

            val poolEntry = PoolEntry(channel, System.currentTimeMillis())
            checkinChannel(poolEntry, isFirstWriteToChannel = true)
          } else {
            log.error(openFuture.getCause, "Error when opening channel to: %s, marking offline".format(address))
            errorStrategy.foreach(_.notifyFailure(request.node))
            poolSize.decrementAndGet

            request.onFailure(openFuture.getCause)
          }
        }
      })
    }
  }

  private def writeRequestToChannel(request: Request[_, _], channel: Channel) {
    log.debug("Writing to %s: %s".format(channel, request))
    requestsSent.incrementAndGet
    channel.write(request).addListener(new ChannelFutureListener {
      def operationComplete(writeFuture: ChannelFuture) = if (!writeFuture.isSuccess) {
        // Take the node out of rotation for a bit
        log.error("IO exception for " + request.node + ", marking node offline")
        errorStrategy.foreach(_.notifyFailure(request.node))
        channel.close


        request.onFailure(writeFuture.getCause)
      }
    })
  }

  def numRequestsSent = requestsSent.get
}

trait ChannelPoolMBean {
  def getOpenChannels: Int
  def getMaxChannels: Int
  def getWriteQueueSize: Int
  def getNumberRequestsSent: Int
}
