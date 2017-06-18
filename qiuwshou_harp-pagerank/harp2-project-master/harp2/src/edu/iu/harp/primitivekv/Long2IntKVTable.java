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
import edu.iu.harp.keyval.KeyValStatus;
import edu.iu.harp.partition.PartitionStatus;
import edu.iu.harp.partition.StructTable;
import edu.iu.harp.resource.ResourcePool;

public class Long2IntKVTable extends
  StructTable<Long2IntKVPartition> {

  private final IntVCombiner combiner;
  private final int expectedKVCount;
  private final int partitionSeed;

  public Long2IntKVTable(IntVCombiner combiner,
    int expectedKVPerPartition,
    int partitionSeed, ResourcePool pool) {
    super(pool);
    this.combiner = combiner;
    this.expectedKVCount = expectedKVPerPartition;
    this.partitionSeed = partitionSeed;
  }

  public void addKeyVal(long key, int val) {
    Long2IntKVPartition partition =
      getOrCreateKVPartition(key);
    addKVInPartition(partition, key, val);
  }

  private KeyValStatus addKVInPartition(
    Long2IntKVPartition partition, long key,
    int val) {
    int curVal = partition.getVal(key);
    if (curVal == -1) {
      partition.putKeyVal(key, val);
      return KeyValStatus.ADDED;
    } else {
      partition.putKeyVal(key,
        combiner.combine(curVal, val));
      return KeyValStatus.COMBINED;
    }
  }

  public int getVal(long key) {
    Long2IntKVPartition partition =
      getKVPartition(key);
    if (partition != null) {
      return partition.getVal(key);
    } else {
      return -1;
    }
  }

  private Long2IntKVPartition
    getOrCreateKVPartition(long key) {
    int partitionID = getKVPartitionID(key);
    Long2IntKVPartition partition =
      this.getPartition(partitionID);
    if (partition == null) {
      partition =
        this.getResourcePool().getWritableObject(
          Long2IntKVPartition.class);
      partition.initialize(partitionID,
        expectedKVCount);
      this.insertPartition(partition);
    }
    return partition;
  }

  private Long2IntKVPartition getKVPartition(
    long key) {
    int partitionID = getKVPartitionID(key);
    Long2IntKVPartition partition =
      this.getPartition(partitionID);
    return partition;
  }

  private int getKVPartitionID(long key) {
    return (int) (key % this.partitionSeed);
  }

  @Override
  protected boolean checkIfPartitionAddable(
    Long2IntKVPartition p) {
    return true;
  }

  @Override
  protected PartitionStatus
    combinePartition(Long2IntKVPartition op,
      Long2IntKVPartition np) {
    Long2IntOpenHashMap nMap = np.getKVMap();
    for (Long2IntMap.Entry entry : nMap
      .long2IntEntrySet()) {
      addKVInPartition(op, entry.getLongKey(),
        entry.getIntValue());
    }
    return PartitionStatus.COMBINED;
  }
}
