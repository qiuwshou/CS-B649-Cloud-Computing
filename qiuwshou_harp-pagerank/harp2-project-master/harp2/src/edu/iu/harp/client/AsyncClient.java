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

import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Communication;
import edu.iu.harp.event.Event;
import edu.iu.harp.event.EventType;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.DataType;
import edu.iu.harp.io.DataUtils;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.Transferable;
import edu.iu.harp.worker.Workers;

/**
 * Asynchronous client. Allows sending events in
 * multiple threads. So this will be non-blocking
 * io,user don't know if sending is failed. the
 * objects sent are automatically released.
 * 
 * @author zhangbj
 *
 */
public class AsyncClient {
  /** Class logger */
  protected static final Logger LOG = Logger
    .getLogger(AsyncClient.class);

  // private final EventQueue eventQueue;
  private final Workers workers;
  private final int selfID;
  private final ResourcePool resourcePool;
  private final int numThreads;
  private final ConcurrentMap<Integer, ConcurrentMap<String, QueueObj>> eventQueueMap;
  private final BlockingQueue<Object> consumerQueue;
  private final ExecutorService taskExecutor;

  private final static int ALL_WORKERS =
    Integer.MAX_VALUE;

  public AsyncClient(/* EventQueue queue, */
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
    taskExecutor =
      Executors.newFixedThreadPool(numThreads);
  }

  private class QueueObj {
    private String contextName;
    private int destID;
    private BlockingQueue<Transferable> queue;
    private AtomicBoolean isConsuming;

    QueueObj(String contextName, int destID) {
      this.contextName = contextName;
      this.destID = destID;
      this.queue = new LinkedBlockingQueue<>();
      this.isConsuming = new AtomicBoolean(false);
    }

    String getContextName() {
      return contextName;
    }

    int getDestID() {
      return destID;
    }

    BlockingQueue<Transferable> getQueue() {
      return queue;
    }

    AtomicBoolean isConsuming() {
      return isConsuming;
    }
  }

  private class SendTask implements Runnable {
    private final int sourceID;

    SendTask(int id) {
      sourceID = id;
    }

    @Override
    public void run() {
      try {
        send();
      } catch (Exception e) {
        LOG.error("Send thread is failed.", e);
      }
    }

    private void send() {
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
        if (obj instanceof QueueObj) {
          QueueObj queueObj = (QueueObj) obj;
          LOG.info("Get queue - context name: "
            + queueObj.getContextName()
            + ", destination: "
            + queueObj.getDestID());
          if (!queueObj.isConsuming()
            .compareAndSet(true, false)) {
            continue;
          }
          LinkedList<Transferable> eventList =
            new LinkedList<>();
          queueObj.getQueue().drainTo(eventList);
          // Put Transferable List to data
          // Send the data
          // Remember that the receiver needs
          // to decode it.
          // And put them back to the event
          // queue as events.
          if (eventList.isEmpty()) {
            continue;
          } else if (eventList.size() == 1) {
            Transferable trans =
              eventList.getFirst();
            if (queueObj.getDestID() == ALL_WORKERS) {
              LOG
                .info("send one collective event");
              Communication.mstBcast(
                queueObj.getContextName(),
                sourceID, null,
                Constants.UNKNOWN_PARTITION_ID,
                trans, false, false, workers,
                resourcePool, true);
              // eventQueue.addEvent(new Event(
              // EventType.COLLECTIVE_EVENT,
              // queueObj.getContextName(),
              // sourceID, trans));
            } else {
              LOG.info("send one message event");
              Communication.send(
                queueObj.getContextName(),
                sourceID, null,
                Constants.UNKNOWN_PARTITION_ID,
                trans, false, false,
                queueObj.getDestID(), workers,
                resourcePool, true);
            }
            // Release this transferable object
            DataUtils.releaseTrans(resourcePool,
              trans);
          } else {
            int eventListSize = eventList.size();
            Data data =
              new Data(DataType.TRANS_LIST,
                queueObj.getContextName(),
                sourceID, eventList);
            if (queueObj.getDestID() == ALL_WORKERS) {
              LOG
                .info("send collective event list "
                  + eventListSize);
              // Broadcast events
              Sender sender =
                new DataMSTBcastSender(data,
                  workers, resourcePool,
                  Constants.MST_BCAST_DECODE);
              sender.execute();
              // data.releaseHeadArray(resourcePool);
              // data
              // .releaseEncodedBody(resourcePool);
              // No add to local once it is sent
              // for (Transferable trans :
              // eventList) {
              // eventQueue.addEvent(new Event(
              // EventType.COLLECTIVE_EVENT,
              // queueObj.getContextName(),
              // sourceID, trans));
              // }
            } else {
              LOG.info("send message event list "
                + eventListSize);
              // Send events to worker ID
              Sender sender =
                new DataSender(data,
                  queueObj.getDestID(), workers,
                  resourcePool,
                  Constants.SEND_DECODE);
              sender.execute();
            }
            data.release(resourcePool);
          }
        } else {
          // Stop Sign
          break;
        }
      }
    }
  }

  public void start() {
    // Submit tasks
    int selfID = workers.getSelfID();
    for (int i = 0; i < numThreads; i++) {
      taskExecutor.submit(new SendTask(selfID));
    }
  }

  /**
   * This method may be invoked in multiple
   * threads. Once an event is submitted, the data
   * contained will be sent and released.
   * 
   * Do not operate on the objects sent as event.
   * 
   * @param events
   */
  public boolean submitMessageEvent(
    int destWorkerID, Event event) {
    // Add event to the queue
    // Message Event to separate worker queues
    // Add queue to the consumer blocking queue
    if (event.getEventType() == EventType.MESSAGE_EVENT
      && event.getBody() != null) {
      if (destWorkerID == selfID) {
        // eventQueue.addEvent(event);
        return false;
      } else {
        QueueObj queueObj =
          getQueueObj(destWorkerID,
            event.getContextName());
        queueObj.getQueue().add(event.getBody());
        // Notify
        if (queueObj.isConsuming().compareAndSet(
          false, true)) {
          consumerQueue.add(queueObj);
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public boolean
    submitCollectiveEvent(Event event) {
    // Add event to the queue
    // Collective event to a special queue
    // Add queue to the consumer blocking queue
    if (event.getEventType() == EventType.COLLECTIVE_EVENT
      && event.getBody() != null) {
      QueueObj queueObj =
        getQueueObj(ALL_WORKERS,
          event.getContextName());
      queueObj.getQueue().add(event.getBody());
      // Notify
      if (queueObj.isConsuming().compareAndSet(
        false, true)) {
        consumerQueue.add(queueObj);
      }
      return true;
    } else {
      return false;
    }
  }

  private QueueObj getQueueObj(int destID,
    String contextName) {
    ConcurrentMap<String, QueueObj> objMap =
      eventQueueMap.get(destID);
    if (objMap == null) {
      objMap =
        new ConcurrentHashMap<>(numThreads,
          Constants.LOAD_FACTOR, numThreads);
      ConcurrentMap<String, QueueObj> oldObjMap =
        eventQueueMap.putIfAbsent(destID, objMap);
      if (oldObjMap != null) {
        objMap = oldObjMap;
      }
    }
    QueueObj obj = objMap.get(contextName);
    if (obj == null) {
      obj = new QueueObj(contextName, destID);
      QueueObj oldObj =
        objMap.putIfAbsent(contextName, obj);
      if (oldObj != null) {
        obj = oldObj;
      }
    }
    return obj;
  }

  public void stop() {
    // Send StopSign to the execution threads
    for (int i = 0; i < numThreads; i++) {
      consumerQueue.add(new ClientStopSign());
    }
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
