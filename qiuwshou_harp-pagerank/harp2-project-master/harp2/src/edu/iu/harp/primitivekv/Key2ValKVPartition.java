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

package edu.iu.harp.primitivekv;

import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import edu.iu.harp.io.Constants;
import edu.iu.harp.keyval.KeyValStatus;
import edu.iu.harp.partition.StructPartition;
import edu.iu.harp.resource.ResourcePool;

/**
 * Each value is a double array... We assume it is
 * not big, each of them just have few elements.
 * 
 * @author zhangbj
 * 
 */
public class Key2ValKVPartition<K extends ObjectK, V extends ObjectV>
  extends StructPartition {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(Key2ValKVPartition.class);

  private Object2ObjectOpenHashMap<K, V> kvMap;
  private Class<K> kClass;
  private Class<V> vClass;
  private LinkedList<K> freeKeys;
  private LinkedList<V> freeVals;

  public Key2ValKVPartition() {
    super();
    setPartitionID(Constants.UNKNOWN_PARTITION_ID);
    kvMap = null;
    kClass = null;
    vClass = null;
    freeKeys = new LinkedList<>();
    freeVals = new LinkedList<>();
  }

  public void initialize(int partitionID,
    int expectedKVCount, Class<K> kClass,
    Class<V> vClass) {
    this.setPartitionID(partitionID);
    if (this.kvMap == null) {
      createKVMap(expectedKVCount);
    }
    this.kClass = kClass;
    this.vClass = vClass;
  }

  protected void createKVMap(int size) {
    this.kvMap =
      new Object2ObjectOpenHashMap<>(size);
    this.kvMap.defaultReturnValue(null);
  }

  public KeyValStatus putKeyVal(K key, V val) {
    if (key == null || val == null) {
      return KeyValStatus.ADD_FAILED;
    }
    if (!freeKeys.isEmpty()
      && !freeVals.isEmpty()) {
      K freeKey = freeKeys.removeFirst();
      V freeVal = freeVals.removeFirst();
      // freeKey.clear();
      // freeVal.clear();
      key.copyTo(freeKey);
      val.copyTo(freeVal);
      this.kvMap.put(freeKey, freeVal);
      return KeyValStatus.COPIED;
    } else {
      this.kvMap.put(key, val);
      return KeyValStatus.ADDED;
    }
  }

  public V removeVal(K key) {
    return this.kvMap.remove(key);
  }

  public Class<K> getKeyClass() {
    return this.kClass;
  }

  public Class<V> getVClass() {
    return this.vClass;
  }

  public V getVal(K key) {
    return this.kvMap.get(key);
  }

  public Object2ObjectOpenHashMap<K, V>
    getKVMap() {
    return kvMap;
  }

  public int size() {
    return this.kvMap.size();
  }

  public boolean isEmpty() {
    return this.kvMap.isEmpty();
  }

  @Override
  public void clear() {
    if (!this.kvMap.isEmpty()) {
      for (Object2ObjectMap.Entry<K, V> entry : this.kvMap
        .object2ObjectEntrySet()) {
        entry.getKey().clear();
        entry.getValue().clear();
        this.freeKeys.add(entry.getKey());
        this.freeVals.add(entry.getValue());
      }
      this.kvMap.clear();
    }
  }

  @Override
  public int getSizeInBytes() {
    // partition ID + mapSize
    int size = 8;
    // kClass name
    size +=
      (this.kClass.getName().length() * 2 + 4);
    // vClass name
    size +=
      (this.vClass.getName().length() * 2 + 4);
    // Key + each array size
    for (Object2ObjectMap.Entry<K, V> entry : this.kvMap
      .object2ObjectEntrySet()) {
      size +=
        (entry.getKey().getSizeInBytes() + entry
          .getValue().getSizeInBytes());
    }
    return size;
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    out.writeInt(getPartitionID());
    out.writeInt(this.kvMap.size());
    out.writeUTF(this.kClass.getName());
    out.writeUTF(this.vClass.getName());
    for (Object2ObjectMap.Entry<K, V> entry : this.kvMap
      .object2ObjectEntrySet()) {
      entry.getKey().write(out);
      entry.getValue().write(out);
    }
  }

  @Override
  public void read(DataInput in)
    throws IOException {
    this.setPartitionID(in.readInt());
    int size = in.readInt();
    if (this.kvMap == null) {
      this.createKVMap(size);
    }
    try {
      this.kClass =
        (Class<K>) Class.forName(in.readUTF());
      this.vClass =
        (Class<V>) Class.forName(in.readUTF());
      K key = null;
      V val = null;
      for (int i = 0; i < size; i++) {
        if (freeKeys.isEmpty()
          && freeVals.isEmpty()) {
          key =
            ResourcePool
              .createObject(this.kClass);
          val =
            ResourcePool
              .createObject(this.vClass);
        } else {
          key = freeKeys.removeFirst();
          val = freeVals.removeFirst();
        }
        key.read(in);
        val.read(in);
        this.kvMap.put(key, val);
      }
    } catch (Exception e) {
      LOG.error("Fail to initialize keyvals.", e);
      throw new IOException(e);
    }
  }
}
