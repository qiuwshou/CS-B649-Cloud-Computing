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

import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import edu.iu.harp.client.DataMSTBcastSender;
import edu.iu.harp.client.DataSender;
import edu.iu.harp.client.Sender;
import edu.iu.harp.comm.Communication;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.DataType;
import edu.iu.harp.io.DataUtils;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.Transferable;
import edu.iu.harp.worker.Workers;

public class SyncQueue {
  /** Class logger */
  protected static final Logger LOG = Logger
    .getLogger(SyncQueue.class);

  private final String contextName;
  private final int sourceID;
  private final int destID;
  private final Workers workers;
  private final ResourcePool resourcePool;
  // private final EventQueue eventQueue;
  private LinkedList<Transferable> queue;
  private LinkedList<Transferable> localList;
  private final BlockingQueue<Object> consumerQueue;
  private boolean inConsumerQueue;
  private boolean isConsuming;
  private final IdentityHashMap<Transferable, CountDownLatch> countDownLatchMap;

  SyncQueue(String contextName, int sourceID,
    int destID, Workers workers,
    ResourcePool resourcePool,
    // EventQueue eventQueue,
    BlockingQueue<Object> consumerQueue) {
    this.contextName = contextName;
    this.sourceID = sourceID;
    this.destID = destID;
    this.workers = workers;
    this.resourcePool = resourcePool;
    // this.eventQueue = eventQueue;
    this.queue = new LinkedList<>();
    this.localList = new LinkedList<>();
    this.consumerQueue = consumerQueue;
    this.inConsumerQueue = false;
    this.isConsuming = false;
    this.countDownLatchMap =
      new IdentityHashMap<>();
  }

  String getContextName() {
    return contextName;
  }

  int getDestID() {
    return destID;
  }

  synchronized EventFuture add(
    Transferable trans, boolean hasFuture) {
    // Only add to the consumer queue if it is
    // taken from the queue
    if (inConsumerQueue) {
      // LOG.info("this queue is in consumer queue");
      queue.addLast(trans);
    } else {
      if (isConsuming) {
        // Add to local list
        // LOG
        // .info("this queue is consuming, add to local list");
        localList.addLast(trans);
      } else {
        // LOG
        // .info("this queue is not consuming, add to consumer queue.");
        queue.addLast(trans);
        consumerQueue.add(this);
        inConsumerQueue = true;
      }
    }
    if (hasFuture) {
      return new EventFuture(true,
        getCountDownLatch(trans));
    } else {
      return null;
    }
  }

  synchronized void enterConsumeBarrier() {
    // Only allow to take when the queue in the
    // consumer queue
    if (inConsumerQueue) {
      inConsumerQueue = false;
      if (!isConsuming) {
        isConsuming = true;
      }
    }
  }

  private synchronized boolean inConsumeBarrier() {
    if (!inConsumerQueue && isConsuming) {
      return true;
    } else {
      return false;
    }
  }

  private synchronized void leaveConsumeBarrier() {
    if (!inConsumerQueue && isConsuming) {
      queue.clear();
      isConsuming = false;
      if (!localList.isEmpty()) {
        // Exchange the local list and the queue
        LinkedList<Transferable> tmpList = null;
        tmpList = queue;
        queue = localList;
        localList = tmpList;
        consumerQueue.add(this);
        inConsumerQueue = true;
      }
    }
  }

  void send() {
    if (!inConsumeBarrier()) {
      return;
    }
    LinkedList<Transferable> transList = queue;
    if (transList.isEmpty()) {
      return;
    }
    if (transList.size() == 1) {
      Transferable trans = transList.getFirst();
      if (destID == SyncClient.ALL_WORKERS) {
        // LOG.info("Send one collective event");
        Communication.mstBcast(contextName,
          sourceID, null,
          Constants.UNKNOWN_PARTITION_ID, trans,
          false, false, workers, resourcePool,
          true);
        // Not sent to local
        // eventQueue.addEvent(new Event(
        // EventType.COLLECTIVE_EVENT,
        // contextName, sourceID, trans));
      } else {
        // boolean isSuccess =
        Communication.send(contextName, sourceID,
          null, Constants.UNKNOWN_PARTITION_ID,
          trans, false, false, destID, workers,
          resourcePool, true);
        // LOG.info("Send a message event: "
        // + isSuccess);
      }
      // Notify the client
      countDown(transList);
      // Release this transferable object
      DataUtils.releaseTrans(resourcePool, trans);
    } else {
      int eventListSize = transList.size();
      Data data =
        new Data(DataType.TRANS_LIST,
          contextName, sourceID, transList);
      if (destID == SyncClient.ALL_WORKERS) {
        // LOG.info("Send collective event list "
        // + eventListSize);
        // Broadcast events
        Sender sender =
          new DataMSTBcastSender(data, workers,
            resourcePool,
            Constants.MST_BCAST_DECODE);
        sender.execute();
        // data.releaseHeadArray(resourcePool);
        // data.releaseEncodedBody(resourcePool);
        // No add to local once it is sent
        // for (Transferable trans : transList) {
        // eventQueue.addEvent(new Event(
        // EventType.COLLECTIVE_EVENT,
        // contextName, sourceID, trans));
        // }
      } else {
        // LOG.info("Send message event list "
        // + eventListSize);
        // Send events to worker ID
        Sender sender =
          new DataSender(data, destID, workers,
            resourcePool, Constants.SEND_DECODE);
        sender.execute();
      }
      // Notify the client
      countDown(transList);
      // Release the data
      data.release(resourcePool);
    }
    leaveConsumeBarrier();
  }

  private synchronized CountDownLatch
    getCountDownLatch(Transferable trans) {
    CountDownLatch latch =
      countDownLatchMap.get(trans);
    if (latch != null) {
      boolean isFailed = false;
      do {
        try {
          latch.await();
          isFailed = false;
        } catch (InterruptedException e) {
          isFailed = true;
          e.printStackTrace();
        }
      } while (isFailed);
    }
    latch = new CountDownLatch(1);
    countDownLatchMap.put(trans, latch);
    return latch;
  }

  private synchronized void countDown(
    LinkedList<Transferable> transList) {
    for (Transferable trans : transList) {
      CountDownLatch latch =
        countDownLatchMap.remove(trans);
      if (latch != null) {
        latch.countDown();
      }
    }
  }
}
