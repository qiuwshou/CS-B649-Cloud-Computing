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

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.iu.harp.compute.ComputeUtil;

public class ParallelCompute<T extends ParallelTask> {
  protected static final Log LOG = LogFactory
    .getLog(ParallelCompute.class);

  private Thread[] threads;
  private final AtomicInteger state;
  private final TaskMonitor[] parallelTasks;
  private final int numParallelTasks;
  private final Semaphore taskEnterBarrier;
  private final Semaphore taskLeaveBarrier;
  private final Semaphore controlSync;

  public ParallelCompute(List<T> tasks) {
    threads = null;
    state = new AtomicInteger(0);
    taskEnterBarrier = new Semaphore(0);
    taskLeaveBarrier = new Semaphore(0);
    controlSync = new Semaphore(0);
    numParallelTasks = tasks.size();
    parallelTasks =
      new TaskMonitor[numParallelTasks];
    int i = 0;
    for (T task : tasks) {
      parallelTasks[i] =
        new TaskMonitor(task, taskEnterBarrier,
          taskLeaveBarrier, controlSync);
      i++;
    }
  }

  @SafeVarargs
  public ParallelCompute(T... tasks) {
    threads = null;
    state = new AtomicInteger(0);
    taskEnterBarrier = new Semaphore(0);
    taskLeaveBarrier = new Semaphore(0);
    controlSync = new Semaphore(0);
    numParallelTasks = tasks.length;
    parallelTasks =
      new TaskMonitor[numParallelTasks];
    for (int i = 0; i < tasks.length; i++) {
      parallelTasks[i] =
        new TaskMonitor(tasks[i],
          taskEnterBarrier, taskLeaveBarrier,
          controlSync);
    }
  }

  public void start() {
    // Starting
    if (state.compareAndSet(0, 1)) {
      threads = new Thread[numParallelTasks];
      int i = 0;
      for (TaskMonitor monitorTask : parallelTasks) {
        monitorTask.setRunning(true);
        threads[i] = new Thread(monitorTask);
        // threads[i]
        // .setPriority(Thread.MAX_PRIORITY);
        threads[i].start();
        i++;
      }
      ComputeUtil.acquire(controlSync,
        numParallelTasks);
      // From starting to idle
      state.compareAndSet(1, 2);
    }
  }

  public void run() {
    // From idle to running
    if (state.compareAndSet(2, 3)) {
      taskEnterBarrier.release(numParallelTasks);
      ComputeUtil.acquire(taskLeaveBarrier,
        numParallelTasks);
      controlSync.release(numParallelTasks);
      state.compareAndSet(3, 2);
    }

  }

  public void launch() {
    // From idle to launch
    if (state.compareAndSet(2, 4)) {
      taskEnterBarrier.release(numParallelTasks);
    }
  }

  public void land() {
    // From launch to land
    if (state.compareAndSet(4, 5)) {
      ComputeUtil.acquire(taskLeaveBarrier,
        numParallelTasks);
      controlSync.release(numParallelTasks);
      state.compareAndSet(5, 2);
    }
  }

  public void stop() {
    // From idle to stop
    if (state.compareAndSet(2, 6)) {
      for (TaskMonitor monitorTask : parallelTasks) {
        monitorTask.setRunning(false);
      }
      taskEnterBarrier.release(numParallelTasks);
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
      state.compareAndSet(6, 0);
    }
  }
}
