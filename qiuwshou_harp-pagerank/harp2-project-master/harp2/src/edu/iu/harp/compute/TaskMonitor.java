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

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

public class TaskMonitor implements Runnable {

  private final BlockingDeque<Object> inputQueue;
  private final BlockingQueue<Object> outputQueue;
  private final ComputeTask taskObject;
  private final Semaphore taskSync;
  private final Semaphore controlSync;

  TaskMonitor(BlockingDeque<Object> inQueue,
    BlockingQueue<Object> outQueue,
    ComputeTask task, Semaphore tSync,
    Semaphore cSync) {
    inputQueue = inQueue;
    outputQueue = outQueue;
    taskObject = task;
    taskSync = tSync;
    controlSync = cSync;
  }

  @Override
  public void run() {
    while (true) {
      try {
        Object input = inputQueue.take();
        if (input != null) {
          if (input.getClass().equals(
            ComputeStopSign.class)) {
            taskSync.release();
            break;
          } else if (input.getClass().equals(
            ComputePauseSign.class)) {
            taskSync.release();
            ComputeUtil.acquire(controlSync);
          } else {
            Object output = null;
            boolean isFailed = false;
            try {
              output = taskObject.run(input);
            } catch (Exception e) {
              output = null;
              isFailed = true;
              e.printStackTrace();
            }
            if (isFailed) {
              outputQueue.add(new ErrorOutput());
            } else if (output == null) {
              // Default null output
              outputQueue.add(new NullOutput());
            } else {
              outputQueue.add(output);
            }
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
        continue;
      }
    }
  }
}
