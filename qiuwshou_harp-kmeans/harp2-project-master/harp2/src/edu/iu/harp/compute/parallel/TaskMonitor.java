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

package edu.iu.harp.compute.parallel;

import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.iu.harp.compute.ComputeUtil;

public class TaskMonitor implements Runnable {
  protected static final Log LOG = LogFactory
    .getLog(TaskMonitor.class);

  private final ParallelTask taskObject;
  private boolean isRunning;
  private final Semaphore taskEnterBarrier;
  private final Semaphore taskLeaveBarrier;
  private final Semaphore controlSync;

  TaskMonitor(ParallelTask task,
    Semaphore tEnterBarrier,
    Semaphore tLeaveBarrier, Semaphore cSync) {
    this.taskObject = task;
    this.isRunning = false;
    this.taskEnterBarrier = tEnterBarrier;
    this.taskLeaveBarrier = tLeaveBarrier;
    this.controlSync = cSync;
  }

  public void setRunning(boolean isRunning) {
    this.isRunning = isRunning;
  }

  @Override
  public void run() {
    controlSync.release();
    while (true) {
      ComputeUtil.acquire(taskEnterBarrier);
      if (isRunning) {
        try {
          taskObject.run();
        } catch (Throwable t) {
          LOG.error("Fail to compute.", t);
        }
        taskLeaveBarrier.release();
        ComputeUtil.acquire(controlSync);
      } else {
        break;
      }
    }
  }
}
