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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import edu.iu.harp.io.Constants;

public class ComputeUtil {

  public static void acquire(Semaphore sem,
    int count) {
    boolean isFailed = false;
    do {
      try {
        sem.acquire(count);
        isFailed = false;
      } catch (Exception e) {
        isFailed = true;
        e.printStackTrace();
      }
    } while (isFailed);
  }

  public static void acquire(Semaphore sem) {
    boolean isFailed = false;
    do {
      try {
        sem.acquire();
        isFailed = false;
      } catch (Exception e) {
        isFailed = true;
        e.printStackTrace();
      }
    } while (isFailed);
  }

  public static void closeExecutor(
    ExecutorService executor) {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(
        Constants.TERMINATION_TIMEOUT_1,
        TimeUnit.SECONDS)) {
        executor.shutdownNow();
        executor.awaitTermination(
          Constants.TERMINATION_TIMEOUT_2,
          TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
