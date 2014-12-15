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
package partitioned
package loadbalancer

import java.security.MessageDigest

/**
 * Object which provides hash function implementations.
 */
object HashFunctions {
  /**
   * A copy of the md5 hash function from the javacompat. This can be used to port your current work from javacompat
   * without changing the hosts that requests will route to.
   *
   * @param bytes the bytes to hash
   *
   * @return the hashed value of the bytes
   *
   */
  def md5[T <% Array[Byte]](bytes: T): Int = {
    try {
      val md = MessageDigest.getInstance("MD5")
      val kbytes: Array[Byte] = md.digest(bytes)
      (kbytes(3) & 0xFF) << 24 | (kbytes(2) & 0xFF) << 16 | (kbytes(1) & 0xFF) << 8 | kbytes(0) & 0xFF
    } catch {
      case e: Exception => throw new RuntimeException(e.getMessage, e)
    }
  }

  /**
   * An implementation of the FNV hash function.
   *
   * @param bytes the bytes to hash
   *
   * @return the hashed value of the bytes
   *
   * @see http://en.wikipedia.org/wiki/Fowler-Noll-Vo_hash_function
   */
  def fnv[T <% Array[Byte]](bytes: T): Int = {
    val FNV_BASIS = 0x811c9dc5
    val FNV_PRIME = (1 << 24) + 0x193

    var hash: Long = FNV_BASIS
    var i: Int = 0
    var maxIdx: Int = bytes.length

    while (i < maxIdx) {
      hash = (hash ^ (0xFF & bytes(i))) * FNV_PRIME
      i += 1
    }
    hash.toInt
  }
}
