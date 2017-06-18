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

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.iu.harp.io.Constants;
import edu.iu.harp.partition.StructPartition;

public class Long2DoubleKVPartition extends
  StructPartition {

  private Long2DoubleOpenHashMap kvMap;
  private final double defaultReturnVal =
    Double.NEGATIVE_INFINITY;

  public Long2DoubleKVPartition() {
    super();
    setPartitionID(Constants.UNKNOWN_PARTITION_ID);
    kvMap = null;
  }

  public void initialize(int partitionID,
    int expectedKVCount) {
    this.setPartitionID(partitionID);
    if (this.kvMap != null) {
      this.kvMap.clear();
    } else {
      this.kvMap =
        new Long2DoubleOpenHashMap(
          expectedKVCount);
      this.kvMap
        .defaultReturnValue(defaultReturnVal);
    }
  }

  public void putKeyVal(long key, double val) {
    this.kvMap.put(key, val);
  }

  public double getVal(long key) {
    return this.kvMap.get(key);
  }

  public Long2DoubleOpenHashMap getKVMap() {
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
    return 8 + kvMap.size() * 16;
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    out.writeInt(getPartitionID());
    out.writeInt(kvMap.size());
    ObjectIterator<Long2DoubleMap.Entry> iterator =
      kvMap.long2DoubleEntrySet().fastIterator();
    while (iterator.hasNext()) {
      Long2DoubleMap.Entry entry =
        iterator.next();
      out.writeLong(entry.getLongKey());
      out.writeDouble(entry.getDoubleValue());
    }
  }

  @Override
  public void read(DataInput in)
    throws IOException {
    this.setPartitionID(in.readInt());
    int size = in.readInt();
    if (kvMap != null) {
      kvMap.clear();
    } else {
      kvMap = new Long2DoubleOpenHashMap(size);
    }
    this.kvMap.defaultReturnValue(0);
    for (int i = 0; i < size; i++) {
      kvMap.put(in.readLong(), in.readDouble());
    }
  }
}
