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

package edu.iu.harp.server;

import java.io.IOException;
import java.io.InputStream;

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
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.ByteArray;

public class DataReceiver extends Receiver {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(DataReceiver.class);

  public DataReceiver(Connection conn,
    EventQueue queue, DataMap map,
    ResourcePool pool, byte commandType) {
    super(conn, queue, map, pool, commandType);
  }

  @Override
  protected void
    handleData(final Connection conn)
      throws Exception {
    InputStream in = conn.getInputDtream();
    // Receive data
    // LOG.info("Start receiving.");
    Data data = receiveData(in);
    // Close connection if no exception thrown
    conn.close();
    // LOG.info("Start decoding.");
    // Decode head array
    data.decodeHeadArray(getResourcePool());
    if (this.getCommandType() == Constants.SEND_DECODE) {
      // Decode data array
      data.releaseHeadArray(getResourcePool());
      data.decodeBodyArray(getResourcePool());
      data.releaseBodyArray(getResourcePool());
    }
    // If the data is not for operation,
    // put it to the queue with message event
    // type
    // If any exception happens in receiving,
    // It throws
    // LOG.info("Start adding.");
    DataUtils.addDataToQueueOrMap(
      this.getEventQueue(),
      EventType.MESSAGE_EVENT, this.getDataMap(),
      data);
  }

  private Data receiveData(final InputStream in)
    throws IOException {
    // Read head array size and body array size
    int headArrSize = -1;
    int bodyArrSize = -1;
    byte[] opBytes =
      this.getResourcePool().getBytes(8);
    try {
      IOUtils.receiveBytes(in, opBytes, 8);
      Deserializer deserializer =
        new Deserializer(new ByteArray(opBytes,
          0, 8));
      headArrSize = deserializer.readInt();
      bodyArrSize = deserializer.readInt();
    } catch (IOException e) {
      // Finally is executed first, then the
      // exception is thrown
      LOG.error("Fail to receive op array", e);
      throw e;
    } finally {
      // Release
      getResourcePool().releaseBytes(opBytes);
    }
    LOG.info("headArrSize: " + headArrSize
      + ", bodyArrSize: " + bodyArrSize);
    // Read head array
    byte[] headBytes = null;
    if (headArrSize > 0) {
      headBytes =
        getResourcePool().getBytes(headArrSize);
      try {
        IOUtils.receiveBytes(in, headBytes,
          headArrSize);
      } catch (Exception e) {
        LOG
          .error("Fail to receive head array", e);
        getResourcePool().releaseBytes(headBytes);
        throw e;
      }
    }
    // Prepare bytes from resource pool
    // Sending or receiving null array is allowed
    byte[] bodyBytes = null;
    if (bodyArrSize > 0) {
      bodyBytes =
        this.getResourcePool().getBytes(
          bodyArrSize);
      try {
        IOUtils.receiveBytes(in, bodyBytes,
          bodyArrSize);
      } catch (Exception e) {
        LOG
          .error("Fail to receive body array", e);
        getResourcePool().releaseBytes(headBytes);
        getResourcePool().releaseBytes(bodyBytes);
        throw e;
      }
    }
    return new Data(new ByteArray(headBytes, 0,
      headArrSize), new ByteArray(bodyBytes, 0,
      bodyArrSize));
  }
}
