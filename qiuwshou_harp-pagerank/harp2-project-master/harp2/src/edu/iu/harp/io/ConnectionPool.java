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

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

public class ConnectionPool {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(ConnectionPool.class);

  private static ConcurrentHashMap<HostPort, PoolObject> connMap =
    new ConcurrentHashMap<>();

  static Connection getConnection(String host,
    int port) {
    Connection conn = null;
    HostPort hostPort = new HostPort(host, port);
    PoolObject poolObj = connMap.get(hostPort);
    if (poolObj == null) {
      poolObj = new PoolObject();
      PoolObject oldPoolObject =
        connMap.putIfAbsent(hostPort, poolObj);
      if (oldPoolObject != null) {
        poolObj = oldPoolObject;
      }
    }
    synchronized (poolObj) {
      boolean getConn = false;
      do {
        if (poolObj.conn == null) {
          LOG.info("Create a new connection to "
            + host + " " + port);
          conn = getNewConnection(host, port);
          poolObj.conn = conn;
          poolObj.inUse = true;
          getConn = true;
        } else if (poolObj.inUse) {
          try {
            poolObj.wait();
          } catch (InterruptedException e) {
          }
        } else {
          conn = poolObj.conn;
          poolObj.inUse = true;
          getConn = true;
        }
      } while (!getConn);
    }
    return conn;
  }

  private static Connection getNewConnection(
    String host, int port) {
    Connection conn = null;
    boolean isFailed = false;
    int count = 0;
    do {
      isFailed = false;
      try {
        conn = new Connection(host, port, 0);
      } catch (Exception e) {
        isFailed = true;
        count++;
        LOG.error("Exception when connecting "
          + host + ":" + port, e);
        try {
          Thread.sleep(Constants.LONG_SLEEP);
        } catch (InterruptedException e1) {
        }
      }
    } while (isFailed
      && count < Constants.SMALL_RETRY_COUNT);
    if (isFailed) {
      LOG.error("Fail to connect " + host + ":"
        + port);
    }
    return conn;
  }

  public static void releaseConnection(
    Connection conn) {
    PoolObject poolObj =
      connMap.get(new HostPort(conn.getNode(),
        conn.getPort()));
    if (poolObj != null) {
      boolean isConn = false;
      synchronized (poolObj) {
        if (conn.equals(poolObj.conn)) {
          poolObj.inUse = false;
          isConn = true;
          poolObj.notifyAll();
          // LOG
          // .info("Release a connection: "
          // + conn.getNode() + " "
          // + conn.getPort());
        }
      }
      if (!isConn) {
        conn.close();
      }
    } else {
      conn.close();
    }
  }

  public static void removeConnection(
    Connection conn) {
    PoolObject poolObj =
      connMap.get(new HostPort(conn.getNode(),
        conn.getPort()));
    if (poolObj != null) {
      synchronized (poolObj) {
        if (conn.equals(poolObj.conn)) {
          poolObj.conn = null;
          poolObj.inUse = false;
          poolObj.notifyAll();
        }
      }
    }
    conn.close();
  }

  /**
   * Close all the connections when no sending is
   * in action.
   */
  public static void closeAllConncetions() {
    for (Entry<HostPort, PoolObject> entry : connMap
      .entrySet()) {
      PoolObject poolObj = entry.getValue();
      synchronized (poolObj) {
        if (poolObj.conn != null) {
          poolObj.conn.close();
          poolObj.conn = null;
          poolObj.inUse = false;
        }
      }
    }
  }
}

class HostPort {
  private String host;
  private int port;

  HostPort(String host, int port) {
    this.host = host;
    this.port = port;
  }

  String getHost() {
    return host;
  }

  int getPort() {
    return port;
  }

  public int hashCode() {
    return port;
  }

  public boolean equals(Object object) {
    if (object instanceof HostPort) {
      HostPort hp = (HostPort) object;
      if (this.host.equals(hp.getHost())
        && this.port == hp.getPort()) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }
}

class PoolObject {

  Connection conn = null;
  boolean inUse = false;

  PoolObject() {
    conn = null;
    inUse = false;
  }
}
