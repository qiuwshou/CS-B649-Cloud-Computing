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

package edu.iu.harp.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import org.apache.log4j.Logger;

public class IOUtils {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(IOUtils.class);

  public static Connection getConnection(
    String host, int port) {
    if (Constants.USE_CACHED_CONNECTION) {
      return ConnectionPool.getConnection(host,
        port);
    } else {
      return startConnection(host, port);
    }
  }

  public static Connection startConnection(
    String host, int port) {
    Connection conn = null;
    boolean isFailed = false;
    int count = 0;
    do {
      isFailed = false;
      try {
        conn = new Connection(host, port, 0);
      } catch (Exception e) {
        // if (e instanceof
        // NoRouteToHostException) {
        // Handle
        // java.net.NoRouteToHostException:
        // Cannot assign requested address
        isFailed = true;
        count++;
        LOG.error("Exception when connecting "
          + host + ":" + port, e);
        try {
          Thread.sleep(Constants.LONG_SLEEP);
        } catch (InterruptedException e1) {
        }
        // }
        // else if (e instanceof ConnectException)
        // {
        // // Handle java.net.ConnectException
        // LOG.error("Exception when connecting "
        // + host + ":" + port + ". "
        // + e.getMessage());
        // }
        // else {
        // LOG.error("Exception when connecting "
        // + host + ":" + port, e);
        // }
      }
    } while (isFailed
      && count < Constants.SMALL_RETRY_COUNT);
    if (isFailed) {
      LOG.error("Fail to connect " + host + ":"
        + port);
    }
    return conn;
  }

  // public static long sendTime = 0L;
  // public static long sendBytes = 0L;
  //
  // public static long getSendTime() {
  // long time = sendTime;
  // sendTime = 0L;
  // return time;
  // }
  //
  // public static long getSendBytes() {
  // long bytes = sendBytes;
  // sendBytes = 0L;
  // return bytes;
  // }

  public static void sendBytes(
    final OutputStream out, final byte[] bytes,
    int start, int size) throws IOException {
    // long t1 = System.currentTimeMillis();
    // while (size > Constants.SEND_BYTE_UNIT) {
    // out.write(bytes, start,
    // Constants.SEND_BYTE_UNIT);
    // start += Constants.SEND_BYTE_UNIT;
    // size -= Constants.SEND_BYTE_UNIT;
    // out.flush();
    // }
    // if (size > 0) {
    // out.write(bytes, start, size);
    // out.flush();
    // }
    out.write(bytes, start, size);
    out.flush();
    // long t2 = System.currentTimeMillis();
    // sendTime += (t2 - t1);
    // sendBytes += size;
  }

  // public static long recvTime = 0L;
  //
  // public static long getRecvTime() {
  // long time = recvTime;
  // recvTime = 0L;
  // return time;
  // }

  public static void receiveBytes(InputStream in,
    byte[] bytes, int size) throws IOException {
    // int start = 0;
    // while (size > Constants.SEND_BYTE_UNIT) {
    // int len =
    // in.read(bytes, start,
    // Constants.SEND_BYTE_UNIT);
    // start += len;
    // size -= len;
    // }
    // while (size > 0) {
    // int len = in.read(bytes, start, size);
    // size -= len;
    // start += len;
    // }
    // long t1 = System.currentTimeMillis();
    int start = 0;
    while (size > 0) {
      int len = in.read(bytes, start, size);
      size -= len;
      start += len;
    }
    // long t2 = System.currentTimeMillis();
    // recvTime += (t2 - t1);
  }

  /**
   * Wait and get a data from DataMap for
   * collective communication
   * 
   * @param workerData
   * @param cClass
   * @param maxTimeOut
   * @param maxWaitCount
   * @return
   * @throws InterruptedException
   */
  public static Data waitAndGet(DataMap dataMap,
    String contextName, String operationName) {
    // LOG.info("Wait data with context name: "
    // + contextName + ", operationName: "
    // + operationName);
    Data data = null;
    int retryCount = 0;
    do {
      try {
        data =
          dataMap.waitAndGetData(contextName,
            operationName,
            Constants.DATA_MAX_WAIT_TIME);
      } catch (InterruptedException e) {
        LOG
          .info("Retry to get data with context name: "
            + contextName
            + ", operationName: "
            + operationName);
        data = null;
        retryCount++;
        if (retryCount == Constants.SMALL_RETRY_COUNT) {
          break;
        }
        continue;
      }
      if (data != null) {
        break;
      }
    } while (true);
    // LOG.info("Get data with context name: "
    // + contextName + ", operationName: "
    // + operationName);
    return data;
  }

  public static void setSocketOptions(
    Socket socket) throws SocketException {
    // LOG.info("Old socket send buffer size: "
    // + socket.getSendBufferSize() + " "
    // + socket.getReceiveBufferSize());
    socket.setKeepAlive(true);
    socket.setReuseAddress(true);
    socket.setTcpNoDelay(true);
    socket
      .setSendBufferSize(Constants.SEND_BYTE_UNIT);
    socket
      .setReceiveBufferSize(Constants.SEND_BYTE_UNIT);
    // LOG.info("New socket send buffer size: "
    // + socket.getSendBufferSize() + " "
    // + socket.getReceiveBufferSize());
  }

  public static void setServerSocketOptions(
    ServerSocket socket) throws SocketException {
    // LOG.info("Old server recv buffer size: "
    // + socket.getReceiveBufferSize());
    socket.setReuseAddress(true);
    socket
      .setReceiveBufferSize(Constants.SEND_BYTE_UNIT);
    // LOG.info("New server recv buffer size: "
    // + socket.getReceiveBufferSize());
  }
}
