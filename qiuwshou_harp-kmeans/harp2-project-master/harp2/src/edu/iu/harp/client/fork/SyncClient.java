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

package edu.iu.harp.client.fork;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.iu.harp.event.Event;
import edu.iu.harp.event.EventType;
import edu.iu.harp.io.Constants;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.Transferable;
import edu.iu.harp.worker.Workers;

/**
 * Synchronous client.
 * 
 * @author zhangbj
 *
 */
public class SyncClient extends RecursiveAction {

  /** Generated serial ID */
  private static final long serialVersionUID =
    484455792633045414L;
  /** Class logger */
  protected static final Logger LOG = Logger
    .getLogger(SyncClient.class);

  // private final EventQueue eventQueue;
  private final Workers workers;
  private final int selfID;
  private final ResourcePool resourcePool;
  private final int numThreads;
  private final ConcurrentMap<Integer, ConcurrentMap<String, SyncQueue>> eventQueueMap;
  private final BlockingQueue<Object> consumerQueue;
  private final ForkJoinPool taskExecutor;

  final static int ALL_WORKERS =
    Integer.MAX_VALUE;

  public SyncClient(/* EventQueue queue, */
  Workers workers, ResourcePool pool,
    int numThreads) {
    // this.eventQueue = queue;
    this.workers = workers;
    this.selfID = workers.getSelfID();
    this.resourcePool = pool;
    this.numThreads = numThreads;
    eventQueueMap =
      new ConcurrentHashMap<>(numThreads,
        Constants.LOAD_FACTOR, numThreads);
    consumerQueue = new LinkedBlockingQueue<>();
    taskExecutor = new ForkJoinPool(numThreads);
  }

  private class SendTask extends RecursiveAction {
    /** Generated serial ID */
    private static final long serialVersionUID =
      2147825101521531758L;

    private final SyncQueue queueObj;

    SendTask(SyncQueue queue) {
      queueObj = queue;
    }

    @Override
    public void compute() {
      try {
        queueObj.enterConsumeBarrier();
        queueObj.send();
      } catch (Exception e) {
        LOG.error("Send thread is failed.", e);
      }
    }
  }

  public EventFuture submitMessageEvent(
    int destWorkerID, Event event,
    boolean hasFuture) {
    // Add event to the queue
    // Message Event to separate worker queues
    // Add queue to the consumer blocking queue
    if (event.getEventType() == EventType.MESSAGE_EVENT
      && event.getBody() != null) {
      if (destWorkerID == selfID) {
        if (hasFuture) {
          return new EventFuture(false, null);
        } else {
          return null;
        }
      } else {
        SyncQueue queueObj =
          getSyncQueue(destWorkerID,
            event.getContextName());
        Transferable trans = event.getBody();
        return queueObj.add(trans, hasFuture);
      }
    } else {
      if (hasFuture) {
        return new EventFuture(false, null);
      } else {
        return null;
      }
    }
  }

  public EventFuture submitCollectiveEvent(
    Event event, boolean hasFuture) {
    // Add event to the queue
    // Collective event to a special queue
    // Add queue to the consumer blocking queue
    if (event.getEventType() == EventType.COLLECTIVE_EVENT
      && event.getBody() != null) {
      SyncQueue queueObj =
        getSyncQueue(ALL_WORKERS,
          event.getContextName());
      Transferable trans = event.getBody();
      return queueObj.add(trans, hasFuture);
    } else {
      if (hasFuture) {
        return new EventFuture(false, null);
      } else {
        return null;
      }
    }
  }

  private SyncQueue getSyncQueue(int destID,
    String contextName) {
    ConcurrentMap<String, SyncQueue> objMap =
      eventQueueMap.get(destID);
    if (objMap == null) {
      objMap =
        new ConcurrentHashMap<>(numThreads,
          Constants.LOAD_FACTOR, numThreads);
      ConcurrentMap<String, SyncQueue> oldObjMap =
        eventQueueMap.putIfAbsent(destID, objMap);
      if (oldObjMap != null) {
        objMap = oldObjMap;
      }
    }
    SyncQueue obj = objMap.get(contextName);
    if (obj == null) {
      obj =
        new SyncQueue(contextName, selfID,
          destID, workers, resourcePool,
          /* eventQueue, */consumerQueue);
      SyncQueue oldObj =
        objMap.putIfAbsent(contextName, obj);
      if (oldObj != null) {
        obj = oldObj;
      }
    }
    return obj;
  }

  public void start() {
    taskExecutor.execute(this);
  }

  @Override
  protected void compute() {
    // Go through each queue
    // drain the queue and send/broadcast
    while (true) {
      Object obj = null;
      try {
        obj = consumerQueue.take();
      } catch (InterruptedException e) {
        e.printStackTrace();
        continue;
      }
      if (obj instanceof SyncQueue) {
        SyncQueue queueObj = (SyncQueue) obj;
        SendTask sendTask =
          new SendTask(queueObj);
        if (numThreads == 1) {
          sendTask.compute();
        } else {
          sendTask.fork();
        }
      } else {
        // Stop Sign
        break;
      }
    }
  }

  public void stop() {
    // Send StopSign to the execution threads
    consumerQueue.add(new ClientStopSign());
    closeExecutor(taskExecutor, "client");
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
}
