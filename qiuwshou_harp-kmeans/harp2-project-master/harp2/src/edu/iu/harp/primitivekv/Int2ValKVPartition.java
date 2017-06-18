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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

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
public class Int2ValKVPartition<V extends ObjectV>
  extends StructPartition {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(Int2ValKVPartition.class);

  private Int2ObjectOpenHashMap<V> kvMap;
  private Class<V> vClass;
  private LinkedList<V> freeVals;

  public Int2ValKVPartition() {
    super();
    setPartitionID(Constants.UNKNOWN_PARTITION_ID);
    kvMap = null;
    vClass = null;
    freeVals = new LinkedList<>();
  }

  public void initialize(int partitionID,
    int expectedKVCount, Class<V> vClass) {
    this.setPartitionID(partitionID);
    if (this.kvMap == null) {
      createKVMap(expectedKVCount);
    }
    this.vClass = vClass;
  }

  protected void createKVMap(int size) {
    this.kvMap =
      new Int2ObjectOpenHashMap<>(size);
    this.kvMap.defaultReturnValue(null);
  }

  public KeyValStatus putKeyVal(int key, V val) {
    if (val == null) {
      return KeyValStatus.ADD_FAILED;
    }
    if (!freeVals.isEmpty()) {
      V freeVal = freeVals.removeFirst();
      val.copyTo(freeVal);
      this.kvMap.put(key, freeVal);
      return KeyValStatus.COPIED;
    } else {
      this.kvMap.put(key, val);
      return KeyValStatus.ADDED;
    }
  }

  public V removeVal(int key) {
    return this.kvMap.remove(key);
  }

  public V getVal(int key) {
    return this.kvMap.get(key);
  }

  public Int2ObjectOpenHashMap<V> getKVMap() {
    return kvMap;
  }

  public Class<V> getVClass() {
    return this.vClass;
  }

  @Override
  public void clear() {
    if (!this.kvMap.isEmpty()) {
      ObjectIterator<Int2ObjectMap.Entry<V>> iterator =
        this.kvMap.int2ObjectEntrySet()
          .fastIterator();
      while (iterator.hasNext()) {
        Int2ObjectMap.Entry<V> entry =
          iterator.next();
        entry.getValue().clear();
        freeVals.add(entry.getValue());
      }
      this.kvMap.clear();
    }
  }

  @Override
  public int getSizeInBytes() {
    // partition ID + mapSize
    int size = 8;
    // vClass name
    size +=
      (this.vClass.getName().length() * 2 + 4);
    size += (this.kvMap.size() * 4);
    // Key + each array size
    ObjectIterator<Int2ObjectMap.Entry<V>> iterator =
      this.kvMap.int2ObjectEntrySet()
        .fastIterator();
    while (iterator.hasNext()) {
      Int2ObjectMap.Entry<V> entry =
        iterator.next();
      size += entry.getValue().getSizeInBytes();
    }
    return size;
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    out.writeInt(getPartitionID());
    out.writeInt(this.kvMap.size());
    out.writeUTF(this.vClass.getName());
    ObjectIterator<Int2ObjectMap.Entry<V>> iterator =
      this.kvMap.int2ObjectEntrySet()
        .fastIterator();
    while (iterator.hasNext()) {
      Int2ObjectMap.Entry<V> entry =
        iterator.next();
      out.writeInt(entry.getIntKey());
      entry.getValue().write(out);
    }
  }

  @Override
  public void read(DataInput in)
    throws IOException {
    this.setPartitionID(in.readInt());
    int size = in.readInt();
    // No matter how much the size is
    // we still need to initialize a map
    if (this.kvMap == null) {
      this.createKVMap(size);
    }
    try {
      this.vClass =
        (Class<V>) Class.forName(in.readUTF());
      for (int i = 0; i < size; i++) {
        V val = null;
        if (freeVals.isEmpty()) {
          val =
            ResourcePool
              .createObject(this.vClass);
        } else {
          val = freeVals.removeFirst();
        }
        int key = in.readInt();
        val.read(in);
        this.kvMap.put(key, val);
      }
    } catch (Exception e) {
      LOG.error("Fail to initialize keyvals.", e);
      throw new IOException(e);
    }
  }
}
