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

package edu.iu.harp.message;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.iu.harp.trans.StructObject;

/**
 * Each partition could have multi-destinations.
 * 
 * @author zhangbj
 *
 */
public class SyncRSAG extends StructObject {
  /** Reduce-scatter target worker IDs */
  private Int2IntOpenHashMap rsTargetWorkerMap;
  /** Allgather partition IDs */
  private IntOpenHashSet agSet;
  /** Which worker this partition goes */
  private Int2ObjectOpenHashMap<IntOpenHashSet> partitionRecvWorkerMap;
  /**
   * How many Data arrays this worker needs to
   * receive
   */
  private Int2IntOpenHashMap workerRecvPartitionCountMap;
  private boolean isSyncAllowed;

  public SyncRSAG() {
    rsTargetWorkerMap = null;
    agSet = null;
    partitionRecvWorkerMap = null;
    workerRecvPartitionCountMap = null;
  }

  public Int2IntOpenHashMap
    getRSTargetWorkerMap() {
    return rsTargetWorkerMap;
  }

  public void setRSTargetWorkerMap(
    Int2IntOpenHashMap rsTargetWorkerMap) {
    this.rsTargetWorkerMap = rsTargetWorkerMap;
  }

  public IntOpenHashSet getAGSet() {
    return agSet;
  }

  public void setAGSet(IntOpenHashSet agSet) {
    this.agSet = agSet;
  }

  public
    void
    setPartitionRecvWorkerMap(
      Int2ObjectOpenHashMap<IntOpenHashSet> partitionRecvWorkerMap) {
    this.partitionRecvWorkerMap =
      partitionRecvWorkerMap;
  }

  public Int2ObjectOpenHashMap<IntOpenHashSet>
    getPartitionTargetWorkerMap() {
    return partitionRecvWorkerMap;
  }

  public
    void
    setWorkerRecvPartitionCountMap(
      Int2IntOpenHashMap workerRecvPartitionCountMap) {
    this.workerRecvPartitionCountMap =
      workerRecvPartitionCountMap;
  }

  public Int2IntOpenHashMap
    getWorkerRecvPartitionCountMap() {
    return workerRecvPartitionCountMap;
  }

  public boolean isSyncAllowed() {
    return isSyncAllowed;
  }

  public void
    setSyncAllowed(boolean isSyncAllowed) {
    this.isSyncAllowed = isSyncAllowed;
  }

  @Override
  public int getSizeInBytes() {
    int size = 4;
    if (rsTargetWorkerMap != null) {
      size += rsTargetWorkerMap.size() * 8;
    }
    size += 4;
    if (agSet != null) {
      size += agSet.size() * 4;
    }
    size += 4;
    if (partitionRecvWorkerMap != null) {
      for (Int2ObjectMap.Entry<IntOpenHashSet> entry : partitionRecvWorkerMap
        .int2ObjectEntrySet()) {
        // Worker ID + array size
        size += (8 + entry.getValue().size() * 4);
      }
    }
    size += 4;
    if (workerRecvPartitionCountMap != null) {
      size +=
        (this.workerRecvPartitionCountMap.size() * 8);
    }
    // Is sync allowed
    size++;
    return size;
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    if (rsTargetWorkerMap != null) {
      out.writeInt(rsTargetWorkerMap.size());
      for (Int2IntMap.Entry entry : rsTargetWorkerMap
        .int2IntEntrySet()) {
        out.writeInt(entry.getIntKey());
        out.writeInt(entry.getIntValue());
      }
    } else {
      out.writeInt(0);
    }
    if (agSet != null) {
      out.writeInt(agSet.size());
      for (int ag : agSet) {
        out.writeInt(ag);
      }
    } else {
      out.writeInt(0);
    }
    if (partitionRecvWorkerMap != null) {
      out.writeInt(partitionRecvWorkerMap.size());
      for (Int2ObjectMap.Entry<IntOpenHashSet> entry : partitionRecvWorkerMap
        .int2ObjectEntrySet()) {
        out.writeInt(entry.getIntKey());
        out.writeInt(entry.getValue().size());
        for (int workerID : entry.getValue()) {
          out.writeInt(workerID);
        }
      }
    } else {
      out.writeInt(0);
    }
    if (workerRecvPartitionCountMap != null) {
      out
        .writeInt(this.workerRecvPartitionCountMap
          .size());
      for (Int2IntMap.Entry entry : this.workerRecvPartitionCountMap
        .int2IntEntrySet()) {
        out.writeInt(entry.getIntKey());
        out.writeInt(entry.getIntValue());
      }
    } else {
      out.writeInt(0);
    }
    out.writeBoolean(this.isSyncAllowed);
  }

  @Override
  public void read(DataInput in)
    throws IOException {
    int size = in.readInt();
    if (size > 0) {
      if (rsTargetWorkerMap == null) {
        rsTargetWorkerMap =
          new Int2IntOpenHashMap(size);
      }
      for (int i = 0; i < size; i++) {
        rsTargetWorkerMap.put(in.readInt(),
          in.readInt());
      }
    } else {
      rsTargetWorkerMap =
        new Int2IntOpenHashMap();
    }
    size = in.readInt();
    if (size > 0) {
      if (agSet == null) {
        agSet = new IntOpenHashSet(size);
      }
      for (int i = 0; i < size; i++) {
        agSet.add(in.readInt());
      }
    } else {
      agSet = new IntOpenHashSet();
    }
    size = in.readInt();
    if (size > 0) {
      if (partitionRecvWorkerMap == null) {
        partitionRecvWorkerMap =
          new Int2ObjectOpenHashMap<>(size);
      }
      int key = 0;
      int setSize = 0;
      IntOpenHashSet set = null;
      for (int i = 0; i < size; i++) {
        key = in.readInt();
        setSize = in.readInt();
        set = new IntOpenHashSet(setSize);
        for (int j = 0; j < setSize; j++) {
          set.add(in.readInt());
        }
        partitionRecvWorkerMap.put(key, set);
      }
    } else {
      partitionRecvWorkerMap =
        new Int2ObjectOpenHashMap<>();
    }
    size = in.readInt();
    if (size > 0) {
      if (workerRecvPartitionCountMap == null) {
        workerRecvPartitionCountMap =
          new Int2IntOpenHashMap(size);
      }
      for (int i = 0; i < size; i++) {
        workerRecvPartitionCountMap.put(
          in.readInt(), in.readInt());
      }
    } else {
      workerRecvPartitionCountMap =
        new Int2IntOpenHashMap();
    }
    this.isSyncAllowed = in.readBoolean();
  }

  @Override
  public void clear() {
    if (rsTargetWorkerMap != null) {
      if (!rsTargetWorkerMap.isEmpty()) {
        rsTargetWorkerMap.clear();
      }
    }
    if (agSet != null) {
      if (!agSet.isEmpty()) {
        agSet.clear();
      }
    }
    if (partitionRecvWorkerMap != null) {
      if (!partitionRecvWorkerMap.isEmpty()) {
        partitionRecvWorkerMap.clear();
      }
    }
    if (workerRecvPartitionCountMap != null) {
      if (!workerRecvPartitionCountMap.isEmpty()) {
        workerRecvPartitionCountMap.clear();
      }
    }
    this.isSyncAllowed = false;
  }
}
