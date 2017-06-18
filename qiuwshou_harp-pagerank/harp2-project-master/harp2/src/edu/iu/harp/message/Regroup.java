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

import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.iu.harp.trans.StructObject;

public class Regroup extends StructObject {
  /** Which worker this partition goes */
  private Int2IntOpenHashMap partitionToWorkerMap;
  /**
   * How many partitions this worker needs to
   * receive
   */
  private Int2IntOpenHashMap workerPartitionCountMap;

  public Regroup() {
    this.partitionToWorkerMap =
      new Int2IntOpenHashMap();
    partitionToWorkerMap.defaultReturnValue(0);
    this.workerPartitionCountMap =
      new Int2IntOpenHashMap();
    workerPartitionCountMap.defaultReturnValue(0);
  }

  public void addPartitionToWorker(
    int partitionID, int destWorkerID) {
    if (!partitionToWorkerMap
      .containsKey(partitionID)) {
      partitionToWorkerMap.put(partitionID,
        destWorkerID);
    }
  }

  public void addWorkerPartitionCount(
    int destWorkerID, int partitionCount) {
    workerPartitionCountMap.addTo(destWorkerID,
      partitionCount);
  }

  public Int2IntOpenHashMap
    getPartitionToWorkerMap() {
    return partitionToWorkerMap;
  }

  public Int2IntOpenHashMap
    getWorkerPartitionCountMap() {
    return workerPartitionCountMap;
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    out.writeInt(partitionToWorkerMap.size());
    for (Entry entry : partitionToWorkerMap
      .int2IntEntrySet()) {
      out.writeInt(entry.getIntKey());
      out.writeInt(entry.getIntValue());
    }
    out.writeInt(workerPartitionCountMap.size());
    for (Entry entry : workerPartitionCountMap
      .int2IntEntrySet()) {
      out.writeInt(entry.getIntKey());
      out.writeInt(entry.getIntValue());
    }
  }

  @Override
  public void read(DataInput in)
    throws IOException {
    int mapSize = in.readInt();
    if (partitionToWorkerMap == null) {
      partitionToWorkerMap =
        new Int2IntOpenHashMap(mapSize);
    }
    for (int i = 0; i < mapSize; i++) {
      this.partitionToWorkerMap.put(in.readInt(),
        in.readInt());
    }
    mapSize = in.readInt();
    if (workerPartitionCountMap == null) {
      workerPartitionCountMap =
        new Int2IntOpenHashMap(mapSize);
    }
    for (int i = 0; i < mapSize; i++) {
      this.workerPartitionCountMap.put(
        in.readInt(), in.readInt());
    }
  }

  @Override
  public int getSizeInBytes() {
    return 4 + this.partitionToWorkerMap.size()
      * 8 + 4
      + this.workerPartitionCountMap.size() * 8;
  }

  public void clear() {
    this.partitionToWorkerMap.clear();
    this.workerPartitionCountMap.clear();
  }
}
