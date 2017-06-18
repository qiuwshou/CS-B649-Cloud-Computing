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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.iu.harp.io.Connection;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.io.IOUtils;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.worker.Workers;

public class Server implements Runnable {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(Server.class);

  /** Data queue shared with the event machine */
  private final EventQueue eventQueue;
  /**
   * Data map for collective communication
   * operations
   */
  private final DataMap dataMap;
  /** Resource pool */
  private final ResourcePool resourcePool;
  // Executors
  private final ExecutorService serverExecutor;
  private final ExecutorService decodeExecutor;
  /** Make sure the access is synchronized */
  private final Workers workers;

  /**
   * Cache necessary information since "workers"
   * is global
   */
  private final String node;
  private final int port;
  /** Server socket */
  private final ServerSocket serverSocket;

  public Server(String node, int port,
    int numThreads, EventQueue queue,
    DataMap map, Workers workers,
    ResourcePool pool) throws Exception {
    this.eventQueue = queue;
    this.dataMap = map;
    this.resourcePool = pool;
    this.serverExecutor =
      Executors
        .newFixedThreadPool(Integer.MAX_VALUE);
    this.decodeExecutor =
      Executors.newFixedThreadPool(numThreads);
    this.workers = workers;
    // Cache local information
    this.node = node;
    this.port = port;
    // Server socket
    try {
      serverSocket = new ServerSocket();
      IOUtils
        .setServerSocketOptions(serverSocket);
      serverSocket.bind(new InetSocketAddress(
        node, port));
    } catch (Exception e) {
      LOG.error("Error in starting receiver.", e);
      throw new Exception(e);
    }
    LOG.info("Server on " + this.node + " "
      + this.port + " starts.");
  }

  public void start() {
    serverExecutor.execute(this);
  }

  public void stop() {
    closeReceiver(this.node, this.port);
    // Because of the cached connections,
    // we may need to enforce shutting down the
    // executor
    closeExecutor(serverExecutor,
      "serverExecutor");
    closeExecutor(decodeExecutor,
      "decodeExecutor");
    // Close server socket
    try {
      serverSocket.close();
    } catch (IOException e) {
      LOG.error("Fail to stop the server.", e);
    }
    LOG.info("Server ends");
    LOG.info("Server on " + this.node + " "
      + this.port + " is stopped.");
  }

  private void closeReceiver(String ip, int port) {
    Connection conn = null;
    try {
      // Close the receiver on the worker
      conn = new Connection(ip, port, 0);
    } catch (Exception e) {
      conn = null;
      LOG.error("Fail to close the server", e);
    }
    if (conn == null) {
      return;
    }
    try {
      OutputStream out = conn.getOutputStream();
      out.write(Constants.SERVER_QUIT);
      out.flush();
    } catch (Exception e) {
      LOG.error("Fail to close the server", e);
    } finally {
      conn.close();
    }
  }

  private void
    closeExecutor(ExecutorService executor,
      String executorName) {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(
        Constants.TERMINATION_TIMEOUT_1,
        TimeUnit.SECONDS)) {
        LOG.info(executorName
          + " still works after "
          + Constants.TERMINATION_TIMEOUT_1
          + " seconds...");
        executor.shutdownNow();
        if (!executor.awaitTermination(
          Constants.TERMINATION_TIMEOUT_2,
          TimeUnit.SECONDS)) {
          LOG.info(executorName
            + " did not terminate with "
            + Constants.TERMINATION_TIMEOUT_2
            + " more.");
        }
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void run() {
    // All commands should use positive byte
    // integer 0 ~ 127
    byte commandType = -1;
    while (true) {
      Connection conn = null;
      try {
        Socket socket = serverSocket.accept();
        IOUtils.setSocketOptions(socket);
        OutputStream out =
          socket.getOutputStream();
        InputStream in = socket.getInputStream();
        // Receiver connection
        conn =
          new Connection(this.node, this.port,
            out, in, socket);
        commandType = (byte) in.read();
      } catch (Exception e) {
        LOG.error("Exception on Server", e);
        if (conn != null) {
          conn.close();
        }
        continue;
      }
      if (commandType == Constants.SERVER_QUIT) {
        conn.close();
        break;
      } else {
        Acceptor acceptor =
          new Acceptor(conn, eventQueue, dataMap,
            workers, resourcePool, commandType,
            decodeExecutor);
        serverExecutor.submit(acceptor);
      }
    }
  }
}