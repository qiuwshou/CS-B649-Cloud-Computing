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

package edu.iu.harp.compute;

import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Computation<T extends ComputeTask> {
  protected static final Log LOG = LogFactory
    .getLog(Computation.class);

  private final BlockingDeque<Object> inputQueue;
  private final BlockingQueue<Object> outputQueue;

  private Thread[] threads;
  private int inputCount;
  private int outputCount;
  private boolean isRunning;
  private boolean isPausing;
  private final TaskMonitor[] computeTasks;
  private final int numComputeTasks;
  private final Semaphore taskSync;
  private final Semaphore controlSync;

  public Computation(List<T> tasks) {
    inputQueue = new LinkedBlockingDeque<>();
    outputQueue = new LinkedBlockingQueue<>();
    threads = null;
    inputCount = 0;
    outputCount = 0;
    isRunning = false;
    isPausing = false;
    taskSync = new Semaphore(0);
    controlSync = new Semaphore(0);
    numComputeTasks = tasks.size();
    computeTasks =
      new TaskMonitor[numComputeTasks];
    int i = 0;
    for (T task : tasks) {
      computeTasks[i] =
        new TaskMonitor(inputQueue, outputQueue,
          task, taskSync, controlSync);
      i++;
    }
  }

  @SafeVarargs
  public Computation(T... tasks) {
    inputQueue = new LinkedBlockingDeque<>();
    outputQueue = new LinkedBlockingQueue<>();
    threads = null;
    inputCount = 0;
    outputCount = 0;
    isRunning = false;
    isPausing = false;
    taskSync = new Semaphore(0);
    controlSync = new Semaphore(0);
    numComputeTasks = tasks.length;
    computeTasks =
      new TaskMonitor[numComputeTasks];
    for (int i = 0; i < tasks.length; i++) {
      computeTasks[i] =
        new TaskMonitor(inputQueue, outputQueue,
          tasks[i], taskSync, controlSync);
    }
  }

  public synchronized void submit(Object input) {
    // Submit inputs
    inputQueue.add(input);
    if (isRunning) {
      inputCount++;
    }
  }

  public synchronized <O extends Object> void
    submitAll(List<O> input) {
    // Submit inputs
    inputQueue.addAll(input);
    if (isRunning) {
      inputCount += input.size();
    }
  }

  public synchronized <O extends Object> void
    submitAll(O[] input) {
    // Submit inputs
    int submitCount = 0;
    for (O object : input) {
      if (object != null) {
        inputQueue.add(object);
        submitCount++;
      }
    }
    if (isRunning) {
      inputCount += submitCount;
    }
  }

  public synchronized void start() {
    // Start monitor threads, wait for inputs
    if (!isRunning) {
      isRunning = true;
      inputCount += inputQueue.size();
      if (isPausing) {
        isPausing = false;
        controlSync.release(numComputeTasks);
      } else {
        threads = new Thread[numComputeTasks];
        for (int i = 0; i < numComputeTasks; i++) {
          threads[i] =
            new Thread(computeTasks[i]);
          threads[i].start();
        }
      }
    }
  }

  public synchronized void pause() {
    if (isRunning && !isPausing) {
      isRunning = false;
      isPausing = true;
      for (int i = 0; i < numComputeTasks; i++) {
        inputQueue
          .addFirst(new ComputePauseSign());
      }
      ComputeUtil.acquire(taskSync,
        numComputeTasks);
      inputCount -= inputQueue.size();
    }
  }

  public synchronized void waitAndPause() {
    if (isRunning && !isPausing) {
      isRunning = false;
      isPausing = true;
      for (int i = 0; i < numComputeTasks; i++) {
        inputQueue
          .addLast(new ComputePauseSign());
      }
      ComputeUtil.acquire(taskSync,
        numComputeTasks);
      inputCount -= inputQueue.size();
    }
  }

  public synchronized void stop() {
    // Stop submission
    // Send StopSign to the queue
    // based on the number of tasks
    if (isPausing) {
      start();
    }
    if (isRunning) {
      isRunning = false;
      for (int i = 0; i < numComputeTasks; i++) {
        inputQueue.addLast(new ComputeStopSign());
      }
      ComputeUtil.acquire(taskSync,
        numComputeTasks);
      for (int i = 0; i < threads.length; i++) {
        boolean isFailed = false;
        do {
          isFailed = false;
          try {
            threads[i].join();
          } catch (Exception e) {
            isFailed = true;
          }
        } while (isFailed);
      }
      threads = null;
    }
  }

  public synchronized Object waitOutput() {
    // If no output is available, wait for one
    Object output = null;
    if (hasOutput()) {
      boolean isFailed = false;
      do {
        try {
          output = outputQueue.take();
          isFailed = false;
        } catch (Exception e) {
          if (output == null) {
            isFailed = true;
          } else {
            isFailed = false;
          }
          // Interrupt may happen when waiting for
          // an element is available
          e.printStackTrace();
        }
      } while (isFailed);
      outputCount++;
    }
    return output;
  }

  private boolean hasOutput() {
    return inputCount > outputCount;
  }
}
