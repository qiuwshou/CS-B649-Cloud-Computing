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
import it.unimi.dsi.fastutil.ints.IntArrayList;

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
public class Sync extends StructObject {
  /** Which worker this partition goes */
  private Int2ObjectOpenHashMap<IntArrayList> partitionToWorkerMap;
  /**
   * How many Data arrays this worker needs to
   * receive
   */
  private Int2IntOpenHashMap workerPartitionCountMap;
  private boolean isSyncAllowed;

  public Sync() {
    partitionToWorkerMap = null;
    workerPartitionCountMap = null;
  }

  public
    void
    setPartitionToWorkerMap(
      Int2ObjectOpenHashMap<IntArrayList> partitionToWorkerMap) {
    this.partitionToWorkerMap =
      partitionToWorkerMap;
  }

  public Int2ObjectOpenHashMap<IntArrayList>
    getPartitionToWorkerMap() {
    return partitionToWorkerMap;
  }

  public void setWorkerPartitionCountMap(
    Int2IntOpenHashMap workerPartitionCountMap) {
    this.workerPartitionCountMap =
      workerPartitionCountMap;
  }

  public Int2IntOpenHashMap
    getWorkerPartitionCountMap() {
    return workerPartitionCountMap;
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
    if (partitionToWorkerMap != null) {
      for (Int2ObjectMap.Entry<IntArrayList> entry : partitionToWorkerMap
        .int2ObjectEntrySet()) {
        // Worker ID + array size
        size += (8 + entry.getValue().size() * 4);
      }
    }
    size += 4;
    if (workerPartitionCountMap != null) {
      size +=
        (this.workerPartitionCountMap.size() * 8);
    }
    // Is sync allowed
    size++;
    return size;
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    if (partitionToWorkerMap != null) {
      out.writeInt(partitionToWorkerMap.size());
      for (Int2ObjectMap.Entry<IntArrayList> entry : partitionToWorkerMap
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
    if (workerPartitionCountMap != null) {
      out.writeInt(this.workerPartitionCountMap
        .size());
      for (Int2IntMap.Entry entry : this.workerPartitionCountMap
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
    int mapSize = in.readInt();
    if (mapSize > 0) {
      if (partitionToWorkerMap == null) {
        partitionToWorkerMap =
          new Int2ObjectOpenHashMap<IntArrayList>(
            mapSize);
      }
    }
    int key = 0;
    int listSize = 0;
    IntArrayList list = null;
    for (int i = 0; i < mapSize; i++) {
      key = in.readInt();
      listSize = in.readInt();
      list = new IntArrayList(listSize);
      for (int j = 0; j < listSize; j++) {
        list.add(in.readInt());
      }
      partitionToWorkerMap.put(key, list);
    }
    mapSize = in.readInt();
    if (mapSize > 0) {
      if (workerPartitionCountMap == null) {
        workerPartitionCountMap =
          new Int2IntOpenHashMap(mapSize);
      }
    }
    for (int i = 0; i < mapSize; i++) {
      workerPartitionCountMap.put(in.readInt(),
        in.readInt());
    }
    this.isSyncAllowed = in.readBoolean();
  }

  @Override
  public void clear() {
    if (partitionToWorkerMap != null) {
      if (!partitionToWorkerMap.isEmpty()) {
        partitionToWorkerMap.clear();
      }
    }
    if (workerPartitionCountMap != null) {
      if (!workerPartitionCountMap.isEmpty()) {
        workerPartitionCountMap.clear();
      }
    }
    this.isSyncAllowed = false;
  }
}
