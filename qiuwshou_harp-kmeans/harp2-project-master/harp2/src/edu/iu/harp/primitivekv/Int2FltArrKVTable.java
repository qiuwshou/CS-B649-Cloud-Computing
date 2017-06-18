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
import edu.iu.harp.utils.Int2ObjValHeldHashMap;

/**
 * a key-value table with int as key and a float
 * array as value
 * 
 * @author zhangbj
 */
public class Int2FltArrKVTable extends
  StructTable<Int2FltArrKVPartition> {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(Int2FltArrKVTable.class);

  private final int arrLen;
  private final FloatArrVCombiner combiner;
  private final int expectedKVCount;
  private final int partitionSeed;

  public Int2FltArrKVTable(int arrLen,
    FloatArrVCombiner combiner,
    int expectedKVPerPartition,
    int partitionSeed, ResourcePool pool) {
    super(pool);
    this.arrLen = arrLen;
    this.combiner = combiner;
    this.expectedKVCount = expectedKVPerPartition;
    this.partitionSeed = partitionSeed;
  }

  public int getFloatArrLen() {
    return this.arrLen;
  }

  public KeyValStatus
    addKeyVal(int key, float[] val) {
    Int2FltArrKVPartition partition =
      getOrCreateKVPartition(key);
    return addKVInPartition(partition, key, val);
  }

  private KeyValStatus addKVInPartition(
    Int2FltArrKVPartition partition, int key,
    float[] val) {
    float[] curVal = partition.getVal(key);
    if (curVal == null) {
      return partition.putKeyVal(key, val);
    } else {
      // Try to combine v to value
      return combiner.combine(curVal, val);
    }
  }

  public float[] getVal(int key) {
    Int2FltArrKVPartition partition =
      getKVPartition(key);
    if (partition != null) {
      return partition.getVal(key);
    } else {
      return null;
    }
  }

  private Int2FltArrKVPartition
    getOrCreateKVPartition(int key) {
    int partitionID = getKVPartitionID(key);
    Int2FltArrKVPartition partition =
      this.getPartition(partitionID);
    if (partition == null) {
      partition =
        this.getResourcePool().getWritableObject(
          Int2FltArrKVPartition.class);
      partition.initialize(partitionID,
        expectedKVCount, arrLen);
      this.insertPartition(partition);
    }
    return partition;
  }

  private Int2FltArrKVPartition getKVPartition(
    int key) {
    int partitionID = getKVPartitionID(key);
    Int2FltArrKVPartition partition =
      this.getPartition(partitionID);
    return partition;
  }

  private int getKVPartitionID(int key) {
    return key % this.partitionSeed;
  }

  @Override
  protected boolean checkIfPartitionAddable(
    Int2FltArrKVPartition p) {
    if (p.getFloatArrLength() != this.arrLen) {
      return false;
    }
    return true;
  }

  @Override
  protected PartitionStatus combinePartition(
    Int2FltArrKVPartition op,
    Int2FltArrKVPartition np) {
    Int2ObjValHeldHashMap<float[]> nMap =
      np.getKVMap();
    LinkedList<Integer> rmKeys =
      new LinkedList<>();
    for (Int2ObjValHeldHashMap.Entry<float[]> entry : nMap
      .int2ObjectEntrySet()) {
      int key = entry.getIntKey();
      float[] floatArr = entry.getValue();
      KeyValStatus status =
        addKVInPartition(op, key, floatArr);
      if (status == KeyValStatus.ADDED) {
        rmKeys.add(key);
      }
    }
    for (int key : rmKeys) {
      nMap.remove(key);
    }
    return PartitionStatus.COMBINED;
  }

  public void defragment() {
    for (Int2FltArrKVPartition partition : this
      .getPartitions()) {
      partition.defragment();
    }
  }
}
