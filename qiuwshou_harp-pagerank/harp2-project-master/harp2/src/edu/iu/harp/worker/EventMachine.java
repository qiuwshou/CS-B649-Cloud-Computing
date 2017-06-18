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

package edu.iu.harp.worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.iu.harp.event.Event;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.resource.ResourcePool;

public class EventMachine {

  private EventQueue eventQueue;
  private DataMap dataMap;
  private ResourcePool resourcePool;
  // private Map<Class<Event>, EventHandleMonitor>
  // handlerMap;
  private ExecutorService handlerExecutor;

  private boolean isRunning;

  public EventMachine(EventQueue queue,
    DataMap map, ResourcePool pool) {
    // Store the data coming from the receiver
    eventQueue = queue;
    dataMap = map;
    resourcePool = pool;
    // handlerMap = new HashMap<>();
    // Create an execution pool.
    handlerExecutor =
      Executors.newCachedThreadPool();
    isRunning = false;
  }

  /*
   * public synchronized boolean
   * registerEventHandler(Class<Event> eventClass,
   * EventHandler handler) { if (isRunning) {
   * return false; } // Check if the handler has
   * been registered // register an event class
   * and the related // handle
   * 
   * if (CollectiveEvent.class
   * .isAssignableFrom(eventClass) && handler
   * instanceof CollectiveEventHandler) {
   * handlerMap.put(eventClass, new
   * EventHandleMonitor(eventClass, handler)); }
   * else if (MessageEvent.class
   * .isAssignableFrom(eventClass) && handler
   * instanceof MessageEventHandler) {
   * handlerMap.put(eventClass, new
   * EventHandleMonitor(eventClass, handler)); }
   * else { // cannot register return false; }
   * 
   * return true; }
   */

  public synchronized void startEventProcessing(
    Class<Event> eventClass) {
    // Start an event processing
    // if (isRunning) {
    // handlerExecutor.submit(handlerMap
    // .get(eventClass));
    // }
  }

  public synchronized void stopEventProcessing(
    Class<Event> eventClass) {
    // Stop an event processing
    // if (isRunning) {
    // handlerMap.get(eventClass).stop();
    // }
  }

  private void launchSystemEventHandleMonitor(
    Class<?> eventClass) {
    // EventHandleMonitor monitor = handlerMap
    // .get(eventClass);
    // if (monitor != null) {
    // handlerExecutor.submit(monitor);
    // }
  }

  public void run() {
    // isRunning = true;
    // Do handshake when starting worker?
    // Run StartWorkerEvent handler
    // launchSystemEventHandleMonitor(StartWorkerEvent.class);
    // Get an event from the main loop
    // put to the event queue of an event handler
    // If events process
    // else send to data map
    // while (true) {
    // how to break from the loop?
    // Object data = dataQueue.takeData();
    // if (data instanceof MachineStopSign) {
    // break;
    // } else if (data instanceof Event) {
    // handlerMap.get(data.getClass())
    // .addEventToQueue((Event) data);
    // } else {
    // Add data to data map
    // }
    // If a failure is known
    // Execute RecoverWorkerHandler (coordinate
    // with other workers)
    // }
    // Run StopWorkerEvent handler
    // launchSystemEventHandleMonitor(StopWorkerEvent.class);
    // Wait all the threads end, and then exit
  }
}
