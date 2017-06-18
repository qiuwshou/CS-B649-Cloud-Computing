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

import org.apache.log4j.Logger;

import edu.iu.harp.trans.WritableObject;

public class ResourcePool {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(ResourcePool.class);

  private final ByteArrayPool byteArrays;
  private final IntArrayPool intArrays;
  private final FloatArrayPool floatArrays;
  private final DoubleArrayPool doubleArrays;
  private final WritableObjectPool objects;
  private final ByteArrayOutputStreamPool streams;

  public ResourcePool() {
    byteArrays = new ByteArrayPool();
    intArrays = new IntArrayPool();
    floatArrays = new FloatArrayPool();
    doubleArrays = new DoubleArrayPool();
    objects = new WritableObjectPool();
    streams = new ByteArrayOutputStreamPool();
  }

  public ByteArrayOutputStreamPool
    getByteArrayOutputStreamPool() {
    return streams;
  }

  public byte[] getBytes(int size) {
    return byteArrays.getArray(size);
  }

  public void releaseBytes(byte[] bytes) {
    byteArrays.releaseArrayInUse(bytes);
  }

  public int[] getInts(int size) {
    return intArrays.getArray(size);
  }

  public void releaseInts(int[] ints) {
    intArrays.releaseArrayInUse(ints);
  }

  public float[] getFloats(int size) {
    return floatArrays.getArray(size);
  }

  public void releaseFloats(float[] floats) {
    floatArrays.releaseArrayInUse(floats);
  }

  public double[] getDoubles(int size) {
    return doubleArrays.getArray(size);
  }

  public void releaseDoubles(double[] doubles) {
    doubleArrays.releaseArrayInUse(doubles);
  }

  public static Object createObject(
    String className) {
    try {
      return createObject(Class
        .forName(className));
    } catch (Exception e) {
      LOG.error("Fail to create an object.", e);
      return null;
    }
  }

  public static <T> T
    createObject(Class<T> clazz) {
    T obj = null;
    try {
      // For a random class name, it may not be a
      // WritableObject
      obj = clazz.newInstance();
      // LOG.info("Create a new object " +
      // className + ".");
    } catch (Exception e) {
      // LOG.error("Fail to create object " +
      // className + ".", e);
      obj = null;
    }
    return obj;
  }

  public <W extends WritableObject> W
    getWritableObject(Class<W> wClass) {
    return (W) objects.getWritableObject(wClass
      .getName());
  }

  public <W extends WritableObject> W
    getWritableObject(String className) {
    return (W) objects
      .getWritableObject(className);
  }

  public <W extends WritableObject> void
    releaseWritableObject(W w) {
    objects.releaseWritableObjectInUse(w);
  }

  public void logResourcePoolUsage() {
    LOG.info("\nNumber of byte arrays in use: "
      + byteArrays.getNumArraysInUse() + "\n"
      + "Number of int arrays in use: "
      + intArrays.getNumArraysInUse() + "\n"
      + "Number of float arrays in use: "
      + floatArrays.getNumArraysInUse() + "\n"
      + "Number of double arrays in use: "
      + doubleArrays.getNumArraysInUse() + "\n"
      + "Number of writable objects in use: "
      + objects.getNumObjectsInUse() + "\n");
  }

  public void freeResourcePool() {
    byteArrays.freeAllArrays();
    intArrays.freeAllArrays();
    floatArrays.freeAllArrays();
    doubleArrays.freeAllArrays();
    objects.freeAllWritableObjects();
  }
}
