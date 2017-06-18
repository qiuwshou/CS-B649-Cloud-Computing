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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import edu.iu.harp.io.Connection;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.DataStatus;
import edu.iu.harp.io.IOUtils;
import edu.iu.harp.io.Serializer;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.ByteArray;
import edu.iu.harp.worker.Workers;

/**
 * Different from DataReceiver, we don't release
 * the encoded bytes in failures.
 * 
 */
public class DataSender extends Sender {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(DataSender.class);

  public DataSender(Data data, int destWorkerID,
    Workers workers, ResourcePool pool,
    byte command) {
    super(data, destWorkerID, workers, pool,
      command);
  }

  public DataSender(Data data, String host,
    int port, ResourcePool pool, byte command) {
    super(data, host, port, pool, command);
  }

  @Override
  protected void handleData(
    final Connection conn, final Data data)
    throws Exception {
    // Get head size and body size
    int headArrSize = getHeadSize(data);
    int bodyArrSize = getBodySize(data);
    ByteArray opArray =
      getOPByteArray(headArrSize, bodyArrSize);
    if (opArray == null) {
      throw new IOException(
        "Cannot get op array.");
    }
    try {
      sendDataBytes(conn, opArray, data);
    } catch (IOException e) {
      throw e;
    } finally {
      getResourcePool().releaseBytes(
        opArray.getArray());
    }
  }

  protected int getHeadSize(Data data) {
    return data.getHeadArray().getSize();
  }

  protected int getBodySize(Data data) {
    int bodyArrSize = 0;
    if (data.getBodyStatus() == DataStatus.ENCODED_ARRAY_DECODED
      || data.getBodyStatus() == DataStatus.ENCODED_ARRAY
      || data.getBodyStatus() == DataStatus.ENCODED_ARRAY_DECODE_FAILED) {
      bodyArrSize = data.getBodyArray().getSize();
    } else if (data.getBodyStatus() == DataStatus.ENCODED_STREAM_DECODED) {
      bodyArrSize = data.getBodyStream().size();
    }
    return bodyArrSize;
  }

  protected ByteArray getOPByteArray(
    int headArrSize, int bodyArrSize) {
    byte[] opBytes =
      getResourcePool().getBytes(8);
    try {
      Serializer serializer =
        new Serializer(new ByteArray(opBytes, 0,
          8));
      serializer.writeInt(headArrSize);
      serializer.writeInt(bodyArrSize);
    } catch (Exception e) {
      getResourcePool().releaseBytes(opBytes);
      return null;
    }
    return new ByteArray(opBytes, 0, 8);
  }

  protected void sendDataBytes(Connection conn,
    final ByteArray opArray, final Data data)
    throws IOException {
    OutputStream out = conn.getOutputStream();
    byte[] opBytes = opArray.getArray();
    int opArrSize = opArray.getSize();
    // Get head size and body size
    ByteArray headArray = data.getHeadArray();
    byte[] headBytes = headArray.getArray();
    int headArrSize = headArray.getSize();
    try {
      out.write(getCommand());
      out.flush();
      IOUtils.sendBytes(out, opBytes, 0,
        opArrSize);
      // Send head bytes
      if (headArrSize > 0) {
        IOUtils.sendBytes(out, headBytes, 0,
          headArrSize);
      }
    } catch (IOException e) {
      throw e;
    }
    sendBodyBytes(out, data);
  }

  private void sendBodyBytes(
    final OutputStream out, final Data data)
    throws IOException {
    // Send content data, check the array size
    // first. Sending or receiving null array is
    // allowed
    if (data.getBodyStatus() == DataStatus.ENCODED_ARRAY_DECODED
      || data.getBodyStatus() == DataStatus.ENCODED_ARRAY
      || data.getBodyStatus() == DataStatus.ENCODED_ARRAY_DECODE_FAILED) {
      ByteArray bodyArray = data.getBodyArray();
      if (bodyArray.getSize() > 0) {
        try {
          IOUtils.sendBytes(out,
            bodyArray.getArray(),
            bodyArray.getStart(),
            bodyArray.getSize());
        } catch (IOException e) {
          throw e;
        }
      }
    } else if (data.getBodyStatus() == DataStatus.ENCODED_STREAM_DECODED) {
      ByteArrayOutputStream bodyStream =
        data.getBodyStream();
      if (bodyStream.size() > 0) {
        try {
          sendByteStream(out, bodyStream);
        } catch (IOException e) {
          throw e;
        }
      }
    }
  }

  private void sendByteStream(
    final OutputStream out,
    final ByteArrayOutputStream stream)
    throws IOException {
    stream.writeTo(out);
    out.flush();
  }
}
