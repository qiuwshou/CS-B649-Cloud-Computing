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

package edu.iu.harp.client;

import org.apache.log4j.Logger;

import edu.iu.harp.io.ConnectionPool;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.IOUtils;
import edu.iu.harp.io.Connection;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.DataStatus;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.worker.WorkerInfo;
import edu.iu.harp.worker.Workers;

public abstract class Sender {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(Sender.class);
  private final Data data;
  private final String host;
  private final int port;
  private final int destWorkerID;
  private final Workers workers;
  private final ResourcePool resourcePool;
  private final byte commandType;

  /**
   * To send data between workers, sender and
   * receiver must both have valid IDs.
   * 
   * @param d
   * @param destID
   * @param w
   * @param pool
   * @param command
   */
  public Sender(Data d, int destID, Workers w,
    ResourcePool pool, byte command) {
    WorkerInfo selfInfo = w.getSelfInfo();
    WorkerInfo destInfo = w.getWorkerInfo(destID);
    if (selfInfo == null || destInfo == null) {
      data = null;
      destWorkerID = Constants.UNKNOWN_WORKER_ID;
      workers = null;
      host = null;
      port = Constants.UNKNOWN_PORT;
      resourcePool = null;
      commandType = Constants.UNKNOWN_CMD;
    } else {
      data = d;
      destWorkerID = destID;
      workers = w;
      host = destInfo.getNode();
      port = destInfo.getPort();
      resourcePool = pool;
      commandType = command;
    }
  }

  /**
   * Use host and port directly do send between
   * any to processes It can send data from a
   * process other than workers or send data from
   * a worker to a process outside.
   * 
   * If send data from outside, data's worker ID
   * should be unknown
   * 
   * 
   * @param d
   * @param h
   * @param p
   * @param pool
   * @param command
   */
  public Sender(Data d, String h, int p,
    ResourcePool pool, byte command) {
    data = d;
    destWorkerID = Constants.UNKNOWN_WORKER_ID;
    workers = null;
    host = h;
    port = p;
    resourcePool = pool;
    commandType = command;
  }

  // public static long encodeTime = 0L;
  //
  // public static long getEncodeTime() {
  // long time = encodeTime;
  // encodeTime = 0L;
  // return time;
  // }

  public boolean execute() {
    // Check if we can send
    if (data == null || host == null
      || port == Constants.UNKNOWN_PORT
      || resourcePool == null
      || commandType == Constants.UNKNOWN_CMD) {
      return false;
    }
    if (data.getHeadStatus() == DataStatus.ENCODE_FAILED_DECODED
      || data.getBodyStatus() == DataStatus.ENCODE_FAILED_DECODED
      || data.getHeadStatus() == DataStatus.DECODE_FAILED
      || data.getBodyStatus() == DataStatus.DECODE_FAILED) {
      return false;
    }
    // long t1 = System.currentTimeMillis();
    // Encode head
    if (data.getHeadStatus() == DataStatus.DECODED) {
      DataStatus headStatus =
        data.encodeHead(resourcePool);
      if (headStatus == DataStatus.ENCODE_FAILED_DECODED) {
        return false;
      }
    }
    // Encode body
    if (data.getBodyStatus() == DataStatus.DECODED) {
      DataStatus bodyStatus =
        data.encodeBody(resourcePool);
      if (bodyStatus == DataStatus.ENCODE_FAILED_DECODED) {
        // No generating encoded data
        return false;
      }
    }
    // long t2 = System.currentTimeMillis();
    // encodeTime += (t2 - t1);
    // LOG.info("Start connecting.");
    // Open connection
    Connection conn =
      IOUtils.getConnection(host, port);
    if (conn == null) {
      // Do not release encoded arrays in data.
      return false;
    }
    // LOG.info("Start sending.");
    // Send
    boolean isFailed = false;
    try {
      handleData(conn, data);
      ConnectionPool.releaseConnection(conn);
    } catch (Exception e) {
      LOG.error("Error in sending data.", e);
      ConnectionPool.removeConnection(conn);
      isFailed = true;
    }
    // Do not release encoded arrays in data
    // LOG.info("Finish sending.");
    return !isFailed;
  }

  protected int getDestWorkerID() {
    return this.destWorkerID;
  }

  protected Workers getWorkers() {
    return this.workers;
  }

  protected ResourcePool getResourcePool() {
    return this.resourcePool;
  }

  protected byte getCommand() {
    return this.commandType;
  }

  protected abstract void handleData(
    final Connection conn, final Data data)
    throws Exception;
}
