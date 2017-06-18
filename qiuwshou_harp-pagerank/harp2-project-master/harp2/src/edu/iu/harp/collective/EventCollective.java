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

package edu.iu.harp.collective;

import org.apache.log4j.Logger;

import edu.iu.harp.client.fork.EventFuture;
import edu.iu.harp.client.fork.SyncClient;
import edu.iu.harp.comm.Communication;
import edu.iu.harp.event.Event;
import edu.iu.harp.event.EventType;
import edu.iu.harp.io.ConnectionPool;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.server.reuse.Server;
import edu.iu.harp.trans.DoubleArray;
import edu.iu.harp.worker.Workers;

public class EventCollective {

  /** Class logger */
  protected static final Logger LOG = Logger
    .getLogger(EventCollective.class);

  public static void main(String args[])
    throws Exception {
    String driverHost = args[0];
    int driverPort = Integer.parseInt(args[1]);
    int workerID = Integer.parseInt(args[2]);
    long jobID = Long.parseLong(args[3]);
    // Initialize log
    Driver.initLogger(workerID);
    LOG
      .info("args[] " + driverHost + " "
        + driverPort + " " + workerID + " "
        + jobID);
    // ---------------------------------------------------
    // Worker initialize
    EventQueue eventQueue = new EventQueue();
    DataMap dataMap = new DataMap();
    Workers workers = new Workers(workerID);
    ResourcePool resourcePool =
      new ResourcePool();
    SyncClient client =
      new SyncClient(workers, resourcePool,
        Constants.NUM_SEND_THREADS);
    Server server =
      new Server(workers.getSelfInfo().getNode(),
        workers.getSelfInfo().getPort(),
        Constants.NUM_RECV_THREADS, eventQueue,
        dataMap, workers, resourcePool);
    client.start();
    server.start();
    String contextName = jobID + "";
    // Barrier guarantees the living workers get
    // the same view of the barrier result
    LOG.info("Start Barrier");
    boolean isSuccess =
      Communication
        .barrier(contextName, "barrier", dataMap,
          workers, resourcePool);
    LOG.info("Barrier: " + isSuccess);
    // -----------------------------------------------
    try {
      LOG.info("Start sending message event.");
      int eventCount = 0;
      // Send message event
      for (int i = 0; i < 10; i++) {
        int destID = i % workers.getNumWorkers();
        if (destID != workers.getMasterID()) {
          if (workers.isMaster()) {
            DoubleArray doubleArray =
              BcastCollective
                .generateDoubleArray(100,
                  resourcePool);
            client.submitMessageEvent(destID,
              new Event(EventType.MESSAGE_EVENT,
                contextName, workers.getSelfID(),
                doubleArray), false);
          } else {
            if (destID == workers.getSelfID()) {
              eventCount++;
            }
          }
        }
      }
      // Send collective event
      LOG.info("Start sending collective event.");
      for (int i = 0; i < 10; i++) {
        if (workers.isMaster()) {
          DoubleArray doubleArray =
            BcastCollective.generateDoubleArray(
              100, resourcePool);
          if (i < 4) {
            client.submitCollectiveEvent(
              new Event(
                EventType.COLLECTIVE_EVENT,
                contextName, workers.getSelfID(),
                doubleArray), false);
          } else {
            EventFuture future =
              client.submitCollectiveEvent(
                new Event(
                  EventType.COLLECTIVE_EVENT,
                  contextName, workers
                    .getSelfID(), doubleArray),
                true);
            future.await();
          }
        } else {
          eventCount++;
        }
      }
      // Receive event
      LOG.info("Start receiving. Event count: "
        + eventCount);
      for (int i = 0; i < eventCount; i++) {
        Event event = eventQueue.waitEvent();
        DoubleArray doubleArray =
          (DoubleArray) event.getBody();
        LOG.info("Receive event: "
          + event.getEventType()
          + ". Double Array: First double: "
          + doubleArray.getArray()[doubleArray
            .getStart()]
          + ", last double: "
          + doubleArray.getArray()[doubleArray
            .getStart()
            + doubleArray.getSize()
            - 1]);
        resourcePool.releaseDoubles(doubleArray
          .getArray());
      }
    } catch (Exception e) {
      LOG.error(
        "Error in sending and receiving events.",
        e);
    }
    // -------------------------------------------------------
    Driver.reportToDriver(contextName,
      "report-to-driver", workers.getSelfID(),
      driverHost, driverPort, resourcePool);
    client.stop();
    ConnectionPool.closeAllConncetions();
    server.stop();
    System.exit(0);
  }
}
