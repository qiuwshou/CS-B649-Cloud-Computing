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

package edu.iu.harp.server.fork;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import edu.iu.harp.event.EventType;
import edu.iu.harp.io.Connection;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.DataUtils;
import edu.iu.harp.io.Deserializer;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.io.IOUtils;
import edu.iu.harp.io.Serializer;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.ByteArray;
import edu.iu.harp.worker.WorkerInfo;
import edu.iu.harp.worker.Workers;

public class DataMSTBcastReceiver extends
  Receiver {
  /** Generated serial ID */
  private static final long serialVersionUID =
    8704804447631338948L;
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(DataMSTBcastReceiver.class);

  private final Workers workers;
  private final int selfID;

  /**
   * Throw exception when failing to initialize
   * 
   * @param conn
   * @param queue
   * @param map
   * @param w
   * @param pool
   * @param commandType
   * @throws Exception
   */
  public DataMSTBcastReceiver(Connection conn,
    EventQueue queue, DataMap map, Workers w,
    ResourcePool pool, byte commandType)
    throws Exception {
    super(conn, queue, map, pool, commandType);
    workers = w;
    selfID = w.getSelfID();
    if (selfID == Constants.UNKNOWN_WORKER_ID) {
      throw new Exception(
        "Fail to initialize receiver.");
    }
  }

  @Override
  protected void
    handleData(final Connection conn)
      throws Exception {
    // Receive data
    Data data = receiveData(conn);
    // Close connection if no exception thrown
    // conn.close();
    // Decode head array
    data.decodeHeadArray(getResourcePool());
    if (this.getCommandType() == Constants.MST_BCAST_DECODE) {
      data.releaseHeadArray(getResourcePool());
      // here only body array is decoded
      data.decodeBodyArray(getResourcePool());
      data.releaseBodyArray(getResourcePool());
    }
    // If the data is not for operation,
    // put it to the queue with collective event
    // type
    DataUtils.addDataToQueueOrMap(
      this.getEventQueue(),
      EventType.COLLECTIVE_EVENT,
      this.getDataMap(), data);
  }

  /**
   * Receive 1. command 2. head and body array
   * size 3. head array 4. body array
   * 
   * @param in
   * @param out
   * @return
   * @throws Exception
   */
  private Data receiveData(final Connection conn)
    throws Exception {
    InputStream in = conn.getInputDtream();
    // Read head array size and body array size
    int headArrSize = -1;
    int bodyArrSize = -1;
    int left = -1;
    int right = -1;
    byte[] opBytes =
      this.getResourcePool().getBytes(16);
    try {
      IOUtils.receiveBytes(in, opBytes, 16);
      Deserializer deserializer =
        new Deserializer(new ByteArray(opBytes,
          0, 16));
      headArrSize = deserializer.readInt();
      bodyArrSize = deserializer.readInt();
      left = deserializer.readInt();
      right = deserializer.readInt();
    } catch (Exception e) {
      getResourcePool().releaseBytes(opBytes);
      throw e;
    }
    // LOG.info("headArrSize " + headArrSize
    // + " bodyArrSize " + bodyArrSize);
    // Prepare and receive head array
    byte[] headBytes = null;
    if (headArrSize > 0) {
      headBytes =
        getResourcePool().getBytes(headArrSize);
      try {
        IOUtils.receiveBytes(in, headBytes,
          headArrSize);
      } catch (Exception e) {
        getResourcePool().releaseBytes(opBytes);
        getResourcePool().releaseBytes(headBytes);
        throw e;
      }
    }
    // Prepare and receive body array
    byte[] bodyBytes = null;
    if (bodyArrSize > 0) {
      bodyBytes =
        this.getResourcePool().getBytes(
          bodyArrSize);
      try {
        IOUtils.receiveBytes(in, bodyBytes,
          bodyArrSize);
      } catch (Exception e) {
        getResourcePool().releaseBytes(opBytes);
        getResourcePool().releaseBytes(headBytes);
        getResourcePool().releaseBytes(bodyBytes);
        throw e;
      }
    }
    // Receiving is done
    conn.close();
    if (left < right) {
      // Try to send out,
      // Be careful about the exceptions
      try {
        sendDataInMST(opBytes, headBytes,
          headArrSize, bodyBytes, bodyArrSize,
          left, right);
      } catch (Exception e) {
        getResourcePool().releaseBytes(opBytes);
        getResourcePool().releaseBytes(headBytes);
        getResourcePool().releaseBytes(bodyBytes);
        throw e;
      }
    }
    // Release op bytes
    getResourcePool().releaseBytes(opBytes);
    return new Data(new ByteArray(headBytes, 0,
      headArrSize), new ByteArray(bodyBytes, 0,
      bodyArrSize));
  }

  private void sendDataInMST(byte[] opBytes,
    byte[] headBytes, int headArrSize,
    byte[] bodyBytes, int bodyArrSize, int left,
    int right) {
    // Send data to other nodes
    int middle = (left + right) / 2;
    int half = middle - left + 1;
    int destID = -1;
    int destLeft = -1;
    int destRight = -1;
    while (left < right) {
      // Update destination and the new range
      if (selfID <= middle) {
        destID = selfID + half;
        if (destID > right) {
          destID = right;
        }
        destLeft = middle + 1;
        destRight = right;
        right = middle;
      } else {
        destID = selfID - half;
        destLeft = left;
        destRight = middle;
        left = middle + 1;
      }
      middle = (left + right) / 2;
      half = middle - left + 1;
      // LOG.info("MST Dest ID " + destID + " "
      // + destLeft + " " + destRight);
      Serializer serializer =
        new Serializer(new ByteArray(opBytes, 8,
          16));
      try {
        serializer.writeInt(destLeft);
        serializer.writeInt(destRight);
      } catch (Exception e) {
        // Doesn't want to throw
        continue;
      }
      // Send data to dest
      WorkerInfo destWorker =
        workers.getWorkerInfo(destID);
      if (destWorker != null) {
        Connection destConn =
          IOUtils.startConnection(
            destWorker.getNode(),
            destWorker.getPort());
        if (destConn != null) {
          OutputStream out =
            destConn.getOutputStream();
          // Send head and body array
          if (out != null) {
            try {
              out.write(getCommandType());
              out.write(opBytes);
              out.flush();
              if (headArrSize > 0) {
                out.write(headBytes, 0,
                  headArrSize);
                out.flush();
              }
              if (bodyArrSize > 0) {
                IOUtils.sendBytes(out, bodyBytes,
                  0, bodyArrSize);
              }
            } catch (Exception e) {
              continue;
            } finally {
              destConn.close();
            }
          }
        }
      }
    }
  }
}
