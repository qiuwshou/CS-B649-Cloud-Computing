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

package edu.iu.harp.keyval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.iu.harp.io.Constants;
import edu.iu.harp.partition.StructPartition;
import edu.iu.harp.resource.ResourcePool;

public class KeyValPartition extends
  StructPartition {

  protected static final Logger LOG = Logger
    .getLogger(KeyValPartition.class);

  private HashMap<Key, Value> keyValMap;
  private Class<? extends Key> kClass;
  private Class<? extends Value> vClass;

  public KeyValPartition() {
    super();
    setPartitionID(Constants.UNKNOWN_PARTITION_ID);
    kClass = null;
    vClass = null;
  }

  public void initialize(int partitionID,
    Class<? extends Key> kClass,
    Class<? extends Value> vClass) {
    this.setPartitionID(partitionID);
    if (keyValMap == null) {
      keyValMap = new HashMap<>();
    }
    this.kClass = kClass;
    this.vClass = vClass;
  }

  public Class<? extends Key> getKeyClass() {
    return this.kClass;
  }

  public Class<? extends Value> getValueClass() {
    return this.vClass;
  }

  public boolean isEmpty() {
    return this.keyValMap.isEmpty();
  }

  public void clear() {
    keyValMap.clear();
  }

  void putKeyVal(Key key, Value val) {
    keyValMap.put(key, val);
  }

  public Value getVal(Key key) {
    return keyValMap.get(key);
  }

  public Value removeVal(Key key) {
    return keyValMap.remove(key);
  }

  public Set<Entry<Key, Value>> getKeyVals() {
    return keyValMap.entrySet();
  }

  @Override
  public int getSizeInBytes() {
    // partition id, map size, key
    // and val in map combiner class name
    int size = 8;
    size +=
      (this.kClass.getName().length() * 2 + 4);
    size +=
      (this.vClass.getName().length() * 2 + 4);
    if (this.keyValMap.size() > 0) {
      for (Entry<Key, Value> entry : this.keyValMap
        .entrySet()) {
        size +=
          (entry.getKey().getSizeInBytes() + entry
            .getValue().getSizeInBytes());
      }
    }
    return size;
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    out.writeInt(getPartitionID());
    out.writeInt(keyValMap.size());
    out.writeUTF(this.kClass.getName());
    out.writeUTF(this.vClass.getName());
    if (keyValMap.size() > 0) {
      for (Entry<Key, Value> entry : keyValMap
        .entrySet()) {
        entry.getKey().write(out);
        entry.getValue().write(out);
      }
    }
  }

  @Override
  public void read(DataInput in)
    throws IOException {
    setPartitionID(in.readInt());
    int size = in.readInt();
    try {
      this.kClass =
        (Class<? extends Key>) Class.forName(in
          .readUTF());
      this.vClass =
        (Class<? extends Value>) Class.forName(in
          .readUTF());
      if (size > 0) {
        if (this.keyValMap == null) {
          this.keyValMap = new HashMap<>(size);
        }
        Key k = null;
        Value v = null;
        for (int i = 0; i < size; i++) {
          k =
            ResourcePool
              .createObject(this.kClass);
          v =
            ResourcePool
              .createObject(this.vClass);
          k.read(in);
          v.read(in);
          this.keyValMap.put(k, v);
        }
      }
    } catch (Exception e) {
      LOG.error("Fail to initialize keyvals.", e);
      throw new IOException(e);
    }
  }
}
