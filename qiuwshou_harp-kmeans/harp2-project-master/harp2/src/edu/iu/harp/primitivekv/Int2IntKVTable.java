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
import edu.iu.harp.keyval.KeyValStatus;
import edu.iu.harp.partition.PartitionStatus;
import edu.iu.harp.partition.StructTable;
import edu.iu.harp.resource.ResourcePool;

public class Int2IntKVTable extends
  StructTable<Int2IntKVPartition> {

  private final IntVCombiner combiner;
  private final int expectedKVCount;
  private final int partitionSeed;

  public Int2IntKVTable(IntVCombiner combiner,
    int expectedKVPerPartition,
    int partitionSeed, ResourcePool pool) {
    super(pool);
    this.combiner = combiner;
    this.expectedKVCount = expectedKVPerPartition;
    this.partitionSeed = partitionSeed;
  }

  public void addKeyVal(int key, int val) {
    Int2IntKVPartition partition =
      getOrCreateKVPartition(key);
    addKVInPartition(partition, key, val);
  }

  private KeyValStatus
    addKVInPartition(
      Int2IntKVPartition partition, int key,
      int val) {
    int curVal = partition.getVal(key);
    if (curVal == Integer.MIN_VALUE) {
      partition.putKeyVal(key, val);
      return KeyValStatus.ADDED;
    } else {
      partition.putKeyVal(key,
        combiner.combine(curVal, val));
      return KeyValStatus.COMBINED;
    }
  }

  public int getVal(int key) {
    Int2IntKVPartition partition =
      getKVPartition(key);
    if (partition != null) {
      return partition.getVal(key);
    } else {
      return Integer.MIN_VALUE;
    }
  }

  private Int2IntKVPartition
    getOrCreateKVPartition(int key) {
    int partitionID = getKVPartitionID(key);
    Int2IntKVPartition partition =
      this.getPartition(partitionID);
    if (partition == null) {
      partition =
        this.getResourcePool().getWritableObject(
          Int2IntKVPartition.class);
      partition.initialize(partitionID,
        expectedKVCount);
      this.insertPartition(partition);
    }
    return partition;
  }

  private Int2IntKVPartition getKVPartition(
    int key) {
    int partitionID = getKVPartitionID(key);
    Int2IntKVPartition partition =
      this.getPartition(partitionID);
    return partition;
  }

  protected int getKVPartitionID(int key) {
    return key % this.partitionSeed;
  }

  @Override
  protected boolean checkIfPartitionAddable(
    Int2IntKVPartition p) {
    return true;
  }

  @Override
  protected PartitionStatus combinePartition(
    Int2IntKVPartition op, Int2IntKVPartition np) {
    Int2IntOpenHashMap nMap = np.getKVMap();
    ObjectIterator<Int2IntMap.Entry> iterator =
      nMap.int2IntEntrySet().fastIterator();
    while (iterator.hasNext()) {
      Int2IntMap.Entry entry = iterator.next();
      addKVInPartition(op, entry.getIntKey(),
        entry.getIntValue());
    }
    return PartitionStatus.COMBINED;
  }
}
