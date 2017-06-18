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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.iu.harp.resource.ResourcePool;

public class DataMap {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(DataMap.class);

  private final ConcurrentMap<String, ConcurrentMap<String, BlockingQueue<Data>>> dataMap;
  private final int initialCapacity =
    Constants.NUM_THREADS;
  private final float loadFactor =
    Constants.LOAD_FACTOR;
  private final int concurrencyLevel =
    Constants.NUM_THREADS;

  public DataMap() {
    dataMap =
      new ConcurrentHashMap<>(initialCapacity,
        loadFactor, concurrencyLevel);
  }

  private BlockingQueue<Data>
    createOperationDataQueue(String contextName,
      String operationName) {
    ConcurrentMap<String, BlockingQueue<Data>> opDataMap =
      dataMap.get(contextName);
    if (opDataMap == null) {
      // LOG.info(contextName
      // + " opDataMap doesn't exist");
      opDataMap =
        new ConcurrentHashMap<>(initialCapacity,
          loadFactor, concurrencyLevel);
      ConcurrentMap<String, BlockingQueue<Data>> oldOpDataMap =
        dataMap.putIfAbsent(contextName,
          opDataMap);
      if (oldOpDataMap != null) {
        // LOG.info(contextName +
        // " use old opDataMap");
        opDataMap = oldOpDataMap;
      }
    } else {
      // LOG.info(contextName +
      // " opDataMap exists");
    }
    BlockingQueue<Data> opDataQueue =
      opDataMap.get(operationName);
    if (opDataQueue == null) {
      // LOG.info(operationName +
      // " opDataQueue doesn't exist");
      opDataQueue = new LinkedBlockingQueue<>();
      BlockingQueue<Data> oldOpDataQueue =
        opDataMap.putIfAbsent(operationName,
          opDataQueue);
      if (oldOpDataQueue != null) {
        // LOG.info(operationName +
        // " use old opDataQueue");
        opDataQueue = oldOpDataQueue;
      }
    } else {
      // LOG.info(operationName +
      // " opDataQueue exists");
    }
    return opDataQueue;
  }

  public Data waitAndGetData(String contextName,
    String operationName, long maxWaitTime)
    throws InterruptedException {
    BlockingQueue<Data> opDataQueue =
      createOperationDataQueue(contextName,
        operationName);
    return opDataQueue.poll(maxWaitTime,
      TimeUnit.SECONDS);
  }

  public void putData(Data data) {
    BlockingQueue<Data> opDataQueue =
      createOperationDataQueue(
        data.getContextName(),
        data.getOperationName());
    opDataQueue.add(data);
  }

  public void cleanOperationData(
    String contextName, String operationName,
    ResourcePool pool) {
    ConcurrentMap<String, BlockingQueue<Data>> opDataMap =
      dataMap.get(contextName);
    if (opDataMap != null) {
      BlockingQueue<Data> opDataQueue =
        opDataMap.remove(operationName);
      if (opDataQueue != null) {
        while (!opDataQueue.isEmpty()) {
          opDataQueue.poll().release(pool);
        }
      }
    }
  }

  /**
   * Context is the execution context of the
   * operation. Invoke this when the context is
   * done.
   * 
   */
  public void cleanData(String contextName,
    ResourcePool pool) {
    ConcurrentMap<String, BlockingQueue<Data>> opDataMap =
      dataMap.remove(contextName);
    if (opDataMap != null) {
      for (Entry<String, BlockingQueue<Data>> entry : opDataMap
        .entrySet()) {
        BlockingQueue<Data> opDataQueue =
          entry.getValue();
        while (!opDataQueue.isEmpty()) {
          opDataQueue.poll().release(pool);
        }
      }
    }
  }

  /**
   * If failures happen, the old contexts should
   * all be trashed.
   */
  public void clean(ResourcePool pool) {
    for (Entry<String, ConcurrentMap<String, BlockingQueue<Data>>> entry : dataMap
      .entrySet()) {
      cleanData(entry.getKey(), pool);
    }
    dataMap.clear();
  }
}
