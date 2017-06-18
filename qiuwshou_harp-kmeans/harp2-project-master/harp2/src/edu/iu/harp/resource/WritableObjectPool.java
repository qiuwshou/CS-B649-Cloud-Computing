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

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.iu.harp.trans.WritableObject;

/**
 * Create and manage writable objects
 * 
 */
public class WritableObjectPool {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(WritableObjectPool.class);

  /** A map between ref, and writable objects */
  private HashMap<String, ObjectStore> writableObjectMap;

  private class ObjectStore {
    private IdentityHashMap<WritableObject, Object> inUseMap;
    private LinkedList<WritableObject> freeQueue;

    private ObjectStore() {
      inUseMap = new IdentityHashMap<>();
      freeQueue = new LinkedList<>();
    }
  }

  // Dummy value to associate with an Object in
  // the backing Map
  private static final Object PRESENT =
    new Object();

  public WritableObjectPool() {
    writableObjectMap = new HashMap<>();
  }

  public synchronized WritableObject
    getWritableObject(String className) {
    WritableObject obj = null;
    ObjectStore objectStore =
      writableObjectMap.get(className);
    if (objectStore == null) {
      objectStore = new ObjectStore();
      writableObjectMap.put(className,
        objectStore);
    }
    if (!objectStore.freeQueue.isEmpty()) {
      obj = objectStore.freeQueue.removeFirst();
      if (obj != null) {
        objectStore.inUseMap.put(obj, PRESENT);
        // LOG.info("Get existing object "
        // + className + ".");
      }
    } else {
      // LOG.info("Create a new object " +
      // className + ".");
      obj =
        (WritableObject) ResourcePool
          .createObject(className);
      if (obj != null) {
        objectStore.inUseMap.put(obj, PRESENT);
      }
    }
    return obj;
  }

  public synchronized
    boolean
    releaseWritableObjectInUse(WritableObject obj) {
    if (obj == null) {
      return false;
    }
    ObjectStore objectStore =
      writableObjectMap.get(obj.getClass()
        .getName());
    if (objectStore == null) {
      return false;
    }
    Object rmObject =
      objectStore.inUseMap.remove(obj);
    if (rmObject == null) {
      return false;
    } else {
      obj.clear();
      objectStore.freeQueue.add(obj);
      // LOG.info("Release object "
      // + obj.getClass().getName() + ".");
      return true;
    }
  }

  public synchronized boolean
    freeWritableObjectInUse(WritableObject obj) {
    ObjectStore objectStore =
      writableObjectMap.get(obj.getClass()
        .getName());
    if (objectStore == null) {
      return false;
    }
    Object rmObject =
      objectStore.inUseMap.remove(obj);
    return (rmObject == null);
  }

  public synchronized void
    freeAllWritableObjects() {
    writableObjectMap.clear();
  }

  public synchronized int getNumObjectsInUse() {
    int count = 0;
    for (Entry<String, ObjectStore> entry : writableObjectMap
      .entrySet()) {
      LOG.info("class: " + entry.getKey()
        + ", count: "
        + entry.getValue().inUseMap.size());
      count += entry.getValue().inUseMap.size();
    }
    return count;
  }
}
