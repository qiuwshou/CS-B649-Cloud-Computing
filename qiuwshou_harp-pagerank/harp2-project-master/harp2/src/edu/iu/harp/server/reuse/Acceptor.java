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

import java.io.InputStream;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import edu.iu.harp.io.Connection;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.worker.Workers;

public class Acceptor implements Runnable {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(Acceptor.class);

  private final Connection conn;
  private final EventQueue eventQueue;
  private final DataMap dataMap;
  private final Workers workers;
  private final ResourcePool resourcePool;
  private byte commandType;
  private final ExecutorService decodeExecutor;

  public Acceptor(Connection conn,
    EventQueue queue, DataMap map, Workers w,
    ResourcePool pool, byte command,
    ExecutorService decodeExecutor) {
    this.conn = conn;
    this.eventQueue = queue;
    this.dataMap = map;
    this.workers = w;
    this.resourcePool = pool;
    this.commandType = command;
    this.decodeExecutor = decodeExecutor;
  }

  @Override
  public void run() {
    // All commands should use positive byte
    // integer 0 ~ 127
    InputStream in = conn.getInputDtream();
    try {
      do {
        if (commandType == Constants.SEND) {
          DataReceiver receiver =
            new DataReceiver(conn, eventQueue,
              dataMap, resourcePool,
              Constants.SEND, decodeExecutor);
          receiver.run();
        } else if (commandType == Constants.SEND_DECODE) {
          DataReceiver receiver =
            new DataReceiver(conn, eventQueue,
              dataMap, resourcePool,
              Constants.SEND_DECODE,
              decodeExecutor);
          receiver.run();
        } else if (commandType == Constants.CHAIN_BCAST) {
          Receiver receiver =
            new DataChainBcastReceiver(conn,
              eventQueue, dataMap, workers,
              resourcePool,
              Constants.CHAIN_BCAST,
              decodeExecutor);
          receiver.run();
        } else if (commandType == Constants.CHAIN_BCAST_DECODE) {
          Receiver receiver =
            new DataChainBcastReceiver(conn,
              eventQueue, dataMap, workers,
              resourcePool,
              Constants.CHAIN_BCAST_DECODE,
              decodeExecutor);
          receiver.run();
        } else if (commandType == Constants.MST_BCAST) {
          Receiver receiver =
            new DataMSTBcastReceiver(conn,
              eventQueue, dataMap, workers,
              resourcePool, Constants.MST_BCAST,
              decodeExecutor);
          receiver.run();
        } else if (commandType == Constants.MST_BCAST_DECODE) {
          Receiver receiver =
            new DataMSTBcastReceiver(conn,
              eventQueue, dataMap, workers,
              resourcePool,
              Constants.MST_BCAST_DECODE,
              decodeExecutor);
          receiver.run();
        } else {
          LOG.info("Unknown command: "
            + commandType);
          break;
        }
        commandType = (byte) in.read();
        // LOG.info("Read next command " +
        // commandType);
      } while (true);
    } catch (Exception e) {
      LOG.error("Exception on Acceptor.", e);
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }
}
