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

package edu.iu.harp.server.reuse;

import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import edu.iu.harp.io.Connection;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.resource.ResourcePool;

public abstract class Receiver {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(Receiver.class);

  private final Connection conn;
  private final EventQueue eventQueue;
  private final DataMap dataMap;
  private final ResourcePool pool;
  private final byte commandType;
  private final ExecutorService decodeExecutor;

  public Receiver(Connection conn,
    EventQueue queue, DataMap map,
    ResourcePool pool, byte command,
    ExecutorService decodeExecutor) {
    this.conn = conn;
    this.eventQueue = queue;
    this.dataMap = map;
    this.pool = pool;
    this.commandType = command;
    this.decodeExecutor = decodeExecutor;
  }

  public void run() throws Exception {
    try {
      handleData(conn);
    } catch (Exception e) {
      LOG.error("Exception in handling data", e);
      throw e;
    }
  }

  protected ResourcePool getResourcePool() {
    return this.pool;
  }

  protected byte getCommandType() {
    return this.commandType;
  }

  protected EventQueue getEventQueue() {
    return this.eventQueue;
  }

  protected ExecutorService getDecodeExecutor() {
    return this.decodeExecutor;
  }

  protected DataMap getDataMap() {
    return this.dataMap;
  }

  protected abstract void handleData(
    final Connection conn) throws Exception;
}
