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
import edu.iu.harp.keyval.KeyValStatus;
import edu.iu.harp.partition.PartitionStatus;
import edu.iu.harp.partition.StructTable;
import edu.iu.harp.resource.ResourcePool;

/**
 * This vertex table is for pagerank value
 * 
 * @author zhangbj
 */
public class Long2DoubleKVTable extends
  StructTable<Long2DoubleKVPartition> {

  private final DoubleVCombiner combiner;
  private final int expectedKVCount;
  private final int partitionSeed;

  public Long2DoubleKVTable(
    DoubleVCombiner combiner,
    int expectedKVPerPartition,
    int partitionSeed, ResourcePool pool) {
    super(pool);
    this.combiner = combiner;
    this.expectedKVCount = expectedKVPerPartition;
    this.partitionSeed = partitionSeed;
  }

  public void addKeyVal(long key, double val) {
    Long2DoubleKVPartition partition =
      getOrCreateKVPartition(key);
    addKVInPartition(partition, key, val);
  }

  private KeyValStatus addKVInPartition(
    Long2DoubleKVPartition partition, long key,
    double val) {
    double curVal = partition.getVal(key);
    if (curVal == Double.NEGATIVE_INFINITY) {
      partition.putKeyVal(key, val);
      return KeyValStatus.ADDED;
    } else {
      partition.putKeyVal(key,
        combiner.combine(curVal, val));
      return KeyValStatus.COMBINED;
    }
  }

  public double getVal(long key) {
    Long2DoubleKVPartition partition =
      getKVPartition(key);
    if (partition != null) {
      return partition.getVal(key);
    } else {
      return Double.NEGATIVE_INFINITY;
    }
  }

  private Long2DoubleKVPartition
    getOrCreateKVPartition(long key) {
    int partitionID = getKVPartitionID(key);
    Long2DoubleKVPartition partition =
      this.getPartition(partitionID);
    if (partition == null) {
      partition =
        this.getResourcePool().getWritableObject(
          Long2DoubleKVPartition.class);
      partition.initialize(partitionID,
        expectedKVCount);
      this.insertPartition(partition);
    }
    return partition;
  }

  private Long2DoubleKVPartition getKVPartition(
    long key) {
    int partitionID = getKVPartitionID(key);
    Long2DoubleKVPartition partition =
      this.getPartition(partitionID);
    return partition;
  }

  private int getKVPartitionID(long key) {
    return (int) (key % this.partitionSeed);
  }

  @Override
  protected boolean checkIfPartitionAddable(
    Long2DoubleKVPartition p) {
    return true;
  }

  @Override
  protected PartitionStatus combinePartition(
    Long2DoubleKVPartition op,
    Long2DoubleKVPartition np) {
    Long2DoubleOpenHashMap nMap = np.getKVMap();
    ObjectIterator<Long2DoubleMap.Entry> iterator =
      nMap.long2DoubleEntrySet().fastIterator();
    while (iterator.hasNext()) {
      Long2DoubleMap.Entry entry =
        iterator.next();
      addKVInPartition(op, entry.getLongKey(),
        entry.getDoubleValue());
    }
    return PartitionStatus.COMBINED;
  }
}
