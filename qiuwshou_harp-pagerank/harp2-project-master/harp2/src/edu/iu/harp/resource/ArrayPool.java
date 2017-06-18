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

package edu.iu.harp.resource;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

public abstract class ArrayPool<T> {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(ArrayPool.class);
  // A map between size and buffer array (2
  // hashset. 0: free 1: in-use)
  private Int2ObjectOpenHashMap<ArrayStore> arrayMap;

  private static final int MAX_ARRAY_SIZE =
    Integer.MAX_VALUE - 5;

  private class ArrayStore {
    private LinkedList<T> freeQueue;
    private HashSet<T> inUseSet;

    private ArrayStore() {
      freeQueue = new LinkedList<>();
      inUseSet = new HashSet<>();
    }
  }

  public ArrayPool() {
    arrayMap = new Int2ObjectOpenHashMap<>();
  }

  private int getAdjustedArraySize(int size) {
    int shift =
      32 - Integer.numberOfLeadingZeros(size - 1);
    return shift == 31 ? MAX_ARRAY_SIZE
      : 1 << shift;
  }

  protected abstract T createNewArray(int size);

  protected abstract int getSizeOfArray(T array);

  public synchronized T getArray(int size) {
    int originSize = size;
    if (originSize <= 0) {
      return null;
    }
    // LOG.info("Original size: " + originSize);
    int adjustSize =
      getAdjustedArraySize(originSize);
    // LOG.info("Adjusted size: " + adjustSize);
    if (adjustSize < originSize) {
      return null;
    }
    ArrayStore arrayStore =
      arrayMap.get(adjustSize);
    if (arrayStore == null
      || arrayStore.freeQueue.isEmpty()) {
      T array = null;
      try {
        array = createNewArray(adjustSize);
      } catch (Throwable t) {
        LOG.error(
          "Cannot create array with size "
            + adjustSize
            + ", current total memory: "
            + Runtime.getRuntime().totalMemory()
            + ", current free memory "
            + Runtime.getRuntime().freeMemory(),
          t);
        return null;
      }
      // LOG
      // .info("Create a new array with original size: "
      // + originSize
      // + ", adjusted size: "
      // + adjustSize
      // + ", with type: "
      // + array.getClass().getName());
      if (arrayStore == null) {
        arrayStore = new ArrayStore();
        arrayMap.put(adjustSize, arrayStore);
      }
      arrayStore.inUseSet.add(array);
      return array;
    } else {
      // Move it from available to in-use
      T array =
        arrayStore.freeQueue.removeFirst();
      arrayStore.inUseSet.add(array);
      // LOG
      // .info("Get an existing array with adjusted size: "
      // + adjustSize
      // + ", with type "
      // + array.getClass().getName());
      return array;
    }
  }

  public synchronized boolean releaseArrayInUse(
    T array) {
    if (array == null) {
      LOG.info("Null array.");
      return false;
    }
    int size = getSizeOfArray(array);
    // LOG.info("Release an array with size: "
    // + size + ", with type "
    // + array.getClass().getName());
    ArrayStore arrayStore = arrayMap.get(size);
    if (arrayStore == null) {
      // LOG
      // .info("Fail to release an array with size: "
      // + size
      // + ", with type "
      // + array.getClass().getName()
      // + ". no such an set.");
      return false;
    }
    boolean result =
      arrayStore.inUseSet.remove(array);
    if (!result) {
      // LOG
      // .info("Fail to release an array with size: "
      // + size
      // + ", with type "
      // + array.getClass().getName()
      // + ". no such an array.");
      return false;
    }
    arrayStore.freeQueue.add(array);
    return true;
  }

  public synchronized boolean freeArrayInUse(
    T array) {
    int size = getSizeOfArray(array);
    // LOG.info("Free an array with size: " + size
    // + ", with type "
    // + array.getClass().getName());
    ArrayStore arrayStore = arrayMap.get(size);
    if (arrayStore == null) {
      return false;
    }
    return arrayStore.inUseSet.remove(array);
  }

  public synchronized void freeAllArrays() {
    arrayMap.clear();
  }

  public synchronized int getNumArraysInUse() {
    int count = 0;
    for (Entry<Integer, ArrayStore> entry : arrayMap
      .entrySet()) {
      LOG.info(this + ": size=" + entry.getKey()
        + ", count="
        + entry.getValue().inUseSet.size());
      count += entry.getValue().inUseSet.size();
    }
    return count;
  }
}
