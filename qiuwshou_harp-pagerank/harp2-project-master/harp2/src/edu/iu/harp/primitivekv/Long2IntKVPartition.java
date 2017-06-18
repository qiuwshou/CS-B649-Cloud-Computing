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

import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.iu.harp.io.Constants;
import edu.iu.harp.partition.StructPartition;

public class Long2IntKVPartition extends
  StructPartition {

  private Long2IntOpenHashMap kvMap;

  public Long2IntKVPartition() {
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
        new Long2IntOpenHashMap(expKVCount);
      this.kvMap.defaultReturnValue(-1);
    }
  }

  public void putKeyVal(long key, int val) {
    this.kvMap.put(key, val);
  }

  public int getVal(long key) {
    return this.kvMap.get(key);
  }

  public Long2IntOpenHashMap getKVMap() {
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
    return 8 + kvMap.size() * 12;
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    out.writeInt(getPartitionID());
    out.writeInt(kvMap.size());
    for (Long2IntMap.Entry entry : kvMap
      .long2IntEntrySet()) {
      out.writeLong(entry.getLongKey());
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
      kvMap = new Long2IntOpenHashMap(size);
    }
    this.kvMap.defaultReturnValue(-1);
    for (int i = 0; i < size; i++) {
      kvMap.put(in.readLong(), in.readInt());
    }
  }
}
