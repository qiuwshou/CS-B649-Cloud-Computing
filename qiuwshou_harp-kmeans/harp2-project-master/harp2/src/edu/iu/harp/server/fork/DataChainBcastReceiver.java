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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import edu.iu.harp.event.EventType;
import edu.iu.harp.io.Connection;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.DataUtils;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.io.Deserializer;
import edu.iu.harp.io.IOUtils;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.ByteArray;
import edu.iu.harp.worker.WorkerInfo;
import edu.iu.harp.worker.Workers;

public class DataChainBcastReceiver extends
  Receiver {
  /** Generated serial ID */
  private static final long serialVersionUID =
    6465925417284990242L;
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(DataChainBcastReceiver.class);

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
  public DataChainBcastReceiver(Connection conn,
    EventQueue queue, DataMap map, Workers w,
    ResourcePool pool, byte commandType)
    throws Exception {
    super(conn, queue, map, pool, commandType);
    this.workers = w;
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
    InputStream in = conn.getInputDtream();
    // Receive data
    Data data = receiveData(in);
    // Close connection if no exception thrown
    conn.close();
    // Decode head array
    data.decodeHeadArray(getResourcePool());
    if (this.getCommandType() == Constants.CHAIN_BCAST_DECODE) {
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
  private Data receiveData(final InputStream in)
    throws Exception {
    // Get next worker
    final WorkerInfo next = workers.getNextInfo();
    final int nextID = next.getID();
    final String nextHost = next.getNode();
    final int nextPort = next.getPort();
    // Read head array size and body array size
    int headArrSize = -1;
    int bodyArrSize = -1;
    int sourceID = -1;
    byte[] opBytes =
      this.getResourcePool().getBytes(12);
    try {
      IOUtils.receiveBytes(in, opBytes, 12);
      Deserializer deserializer =
        new Deserializer(new ByteArray(opBytes,
          0, 12));
      headArrSize = deserializer.readInt();
      bodyArrSize = deserializer.readInt();
      sourceID = deserializer.readInt();
    } catch (Exception e) {
      getResourcePool().releaseBytes(opBytes);
      throw e;
    }
    // Get the next connection
    Connection nextConn = null;
    if (sourceID != nextID) {
      nextConn =
        IOUtils.startConnection(nextHost,
          nextPort);
      if (nextConn == null) {
        getResourcePool().releaseBytes(opBytes);
        throw new IOException(
          "Cannot create the next connection.");
      }
    }
    OutputStream out = null;
    if (nextConn != null) {
      out = nextConn.getOutputStream();
    }
    // Prepare and read head array
    byte[] headBytes = null;
    if (headArrSize > 0) {
      headBytes =
        getResourcePool().getBytes(headArrSize);
      try {
        IOUtils.receiveBytes(in, headBytes,
          headArrSize);
        // Forward op bytes and head bytes
        if (out != null) {
          out.write(getCommandType());
          out.write(opBytes, 0, 12);
          out.write(headBytes, 0, headArrSize);
          out.flush();
        }
      } catch (Exception e) {
        getResourcePool().releaseBytes(headBytes);
        if (nextConn != null) {
          nextConn.close();
          nextConn = null;
        }
        throw e;
      } finally {
        getResourcePool().releaseBytes(opBytes);
      }
    }
    // Prepare body array
    byte[] bodyBytes = null;
    if (bodyArrSize > 0) {
      bodyBytes =
        getResourcePool().getBytes(bodyArrSize);
    }
    // Receive and forward body array
    if (bodyArrSize > 0) {
      try {
        receiveBytes(in, out, bodyBytes,
          bodyArrSize);
      } catch (Exception e) {
        getResourcePool().releaseBytes(headBytes);
        getResourcePool().releaseBytes(bodyBytes);
        if (nextConn != null) {
          nextConn.close();
          nextConn = null;
        }
        throw e;
      }
    }
    // Close connection to the next worker
    if (nextConn != null) {
      nextConn.close();
    }
    return new Data(new ByteArray(headBytes, 0,
      headArrSize), new ByteArray(bodyBytes, 0,
      bodyArrSize));
  }

  private void receiveBytes(InputStream in,
    OutputStream out, byte[] bytes, int size)
    throws IOException {
    // Receive bytes data and process
    if (out != null) {
      int start = 0;
      int len = 0;
      while ((start + Constants.SEND_BYTE_UNIT) <= size) {
        len =
          in.read(bytes, start,
            Constants.SEND_BYTE_UNIT);
        if (len > 0) {
          out.write(bytes, start, len);
          out.flush();
        }
        start += len;
      }
      int rest = size - start;
      while (rest > 0) {
        len = in.read(bytes, start, rest);
        if (len > 0) {
          out.write(bytes, start, len);
          out.flush();
        }
        rest -= len;
        start += len;
      }
    } else {
      IOUtils.receiveBytes(in, bytes, size);
    }
  }
}
