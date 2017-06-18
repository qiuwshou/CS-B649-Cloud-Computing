/*
 * Copyright 2014 Indiana University
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.iu.harp.resource;

import java.io.ByteArrayOutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

public class ByteArrayOutputStreamPool {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(ByteArrayOutputStreamPool.class);
  /** 0: available 1: in-use */
  private Set<ByteArrayOutputStream>[] streams;

  public ByteArrayOutputStreamPool() {
    streams = new HashSet[2];
    streams[0] = new HashSet<ByteArrayOutputStream>();
    streams[1] = new HashSet<ByteArrayOutputStream>();
  }

  public synchronized ByteArrayOutputStream
    getByteArrayOutputStream() {
    ByteArrayOutputStream byteOut = null;
    if (streams[0].isEmpty()) {
      byteOut = new ByteArrayOutputStream();
      streams[1].add(byteOut);
      // LOG.info("Create a new ByteArrayOutputStream.");
    } else {
      Iterator<ByteArrayOutputStream> streamIterator = streams[0]
        .iterator();
      byteOut = streamIterator.next();
      streams[0].remove(byteOut);
      streams[1].add(byteOut);
      // LOG.info("Get an existing ByteArrayOutputStream.");
    }
    return byteOut;
  }

  public synchronized boolean
    releaseByteArrayOutputStreamInUse(
      ByteArrayOutputStream stream) {
    stream.reset();
    boolean result = streams[1].remove(stream);
    if (!result) {
      return false;
    }
    streams[0].add(stream);
    return true;
  }

  public synchronized boolean
    freebyteArrayOutputStreamInUse(
      ByteArrayOutputStream stream) {
    return streams[1].remove(stream);
  }

  public synchronized void
    freeAllByteArrayOutputStreams() {
    streams[0].clear();
    streams[1].clear();
  }
}
