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

import java.util.LinkedList;

import org.apache.log4j.Logger;

import edu.iu.harp.keyval.KeyValStatus;
import edu.iu.harp.partition.PartitionStatus;
import edu.iu.harp.partition.StructTable;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.utils.Long2ObjValHeldHashMap;

/**
 * This vertex table is for pagerank value
 * 
 * @author zhangbj
 */
public class LongDblArrKVTable extends
  StructTable<LongDblArrKVPartition> {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(LongDblArrKVTable.class);

  private final int arrLen;
  private final DoubleArrVCombiner combiner;
  private final int expectedKVCount;
  private final int partitionSeed;

  public LongDblArrKVTable(int arrLen,
    DoubleArrVCombiner combiner,
    int expectedKVPerPartition,
    int partitionSeed, ResourcePool pool) {
    super(pool);
    this.arrLen = arrLen;
    this.combiner = combiner;
    this.expectedKVCount = expectedKVPerPartition;
    this.partitionSeed = partitionSeed;
  }

  public int getDoubleArrLen() {
    return this.arrLen;
  }

  public KeyValStatus addKeyVal(long key,
    double[] val) {
    LongDblArrKVPartition partition =
      getOrCreateKVPartition(key);
    return addKVInPartition(partition, key, val);
  }

  private KeyValStatus addKVInPartition(
    LongDblArrKVPartition partition, long key,
    double[] val) {
    double[] curVal = partition.getVal(key);
    if (curVal == null) {
      return partition.putKeyVal(key, val);
    } else {
      // Try to combine v to value
      return combiner.combine(curVal, val);
    }
  }

  public double[] getVal(long key) {
    LongDblArrKVPartition partition =
      getKVPartition(key);
    if (partition != null) {
      return partition.getVal(key);
    } else {
      return null;
    }
  }

  private LongDblArrKVPartition
    getOrCreateKVPartition(long key) {
    int partitionID = getKVPartitionID(key);
    LongDblArrKVPartition partition =
      this.getPartition(partitionID);
    if (partition == null) {
      partition =
        this.getResourcePool().getWritableObject(
          LongDblArrKVPartition.class);
      partition.initialize(partitionID,
        expectedKVCount, arrLen);
      this.insertPartition(partition);
    }
    return partition;
  }

  private LongDblArrKVPartition getKVPartition(
    long key) {
    int partitionID = getKVPartitionID(key);
    LongDblArrKVPartition partition =
      this.getPartition(partitionID);
    return partition;
  }

  private int getKVPartitionID(long key) {
    return (int) (key % this.partitionSeed);
  }

  @Override
  protected boolean checkIfPartitionAddable(
    LongDblArrKVPartition p) {
    if (p.getDoubleArrayLength() != this.arrLen) {
      return false;
    }
    return true;
  }

  @Override
  protected PartitionStatus combinePartition(
    LongDblArrKVPartition op,
    LongDblArrKVPartition np) {
    Long2ObjValHeldHashMap<double[]> nMap =
      np.getKVMap();
    LinkedList<Long> rmKeys = new LinkedList<>();
    for (Long2ObjValHeldHashMap.Entry<double[]> entry : nMap
      .long2ObjectEntrySet()) {
      long key = entry.getLongKey();
      double[] doubleArr = entry.getValue();
      KeyValStatus status =
        addKVInPartition(op, key, doubleArr);
      if (status == KeyValStatus.ADDED) {
        rmKeys.add(key);
      }
    }
    for (long key : rmKeys) {
      nMap.remove(key);
    }
    return PartitionStatus.COMBINED;
  }
}
