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

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.iu.harp.io.Constants;
import edu.iu.harp.partition.StructPartition;

public class Int2IntKVPartition extends
  StructPartition {

  private Int2IntOpenHashMap kvMap;
  private final int defaultReturnVal =
    Integer.MIN_VALUE;

  public Int2IntKVPartition() {
    super();
    setPartitionID(Constants.UNKNOWN_PARTITION_ID);
    kvMap = null;
  }

  public void initialize(int partitionID,
    int expKVCount) {
    this.setPartitionID(partitionID);
    if (this.kvMap != null) {
      if (!this.kvMap.isEmpty()) {
        this.kvMap.clear();
      }
    } else {
      this.kvMap =
        new Int2IntOpenHashMap(expKVCount);
      this.kvMap
        .defaultReturnValue(defaultReturnVal);
    }
  }

  public void putKeyVal(int key, int val) {
    this.kvMap.put(key, val);
  }

  public int getVal(int key) {
    return this.kvMap.get(key);
  }

  public Int2IntOpenHashMap getKVMap() {
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
    this.kvMap.clear();
  }

  @Override
  public int getSizeInBytes() {
    return 8 + kvMap.size() * 8;
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    out.writeInt(getPartitionID());
    out.writeInt(kvMap.size());
    ObjectIterator<Int2IntMap.Entry> iterator =
      kvMap.int2IntEntrySet().fastIterator();
    while (iterator.hasNext()) {
      Int2IntMap.Entry entry = iterator.next();
      out.writeInt(entry.getIntKey());
      out.writeInt(entry.getIntValue());
    }
  }

  @Override
  public void read(DataInput in)
    throws IOException {
    this.setPartitionID(in.readInt());
    int size = in.readInt();
    if (kvMap != null) {
      if (!kvMap.isEmpty()) {
        kvMap.clear();
      }
    } else {
      kvMap = new Int2IntOpenHashMap(size);
    }
    this.kvMap
      .defaultReturnValue(defaultReturnVal);
    for (int i = 0; i < size; i++) {
      kvMap.put(in.readInt(), in.readInt());
    }
  }
}
