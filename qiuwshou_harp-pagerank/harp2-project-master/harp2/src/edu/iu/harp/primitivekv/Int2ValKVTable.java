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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import org.apache.log4j.Logger;

import edu.iu.harp.keyval.KeyValStatus;
import edu.iu.harp.keyval.ValCombiner;
import edu.iu.harp.partition.PartitionStatus;
import edu.iu.harp.partition.StructTable;
import edu.iu.harp.resource.ResourcePool;

/**
 * a key-value table with int as key and a float
 * array as value
 * 
 * @author zhangbj
 */
public class Int2ValKVTable<V extends ObjectV, P extends Int2ValKVPartition<V>>
  extends StructTable<P> {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(Int2ValKVTable.class);

  private final Class<V> vClass;
  private final Class<P> pClass;
  private final ValCombiner<V> combiner;
  private final int expectedKVCountPerPartition;
  private final int partitionSeed;

  public Int2ValKVTable(Class<V> vClass,
    Class<P> pClass, ValCombiner<V> combiner,
    int expectedKVPerPartition,
    int partitionSeed, ResourcePool pool) {
    super(pool);
    this.vClass = vClass;
    this.pClass = pClass;
    this.combiner = combiner;
    this.expectedKVCountPerPartition =
      expectedKVPerPartition;
    this.partitionSeed = partitionSeed;
  }

  public KeyValStatus addKeyVal(int key, V val) {
    P partition = getOrCreateKVPartition(key);
    return addKVInPartition(partition, key, val);
  }

  private KeyValStatus addKVInPartition(
    P partition, int key, V val) {
    V curVal = partition.getVal(key);
    if (curVal == null) {
      return partition.putKeyVal(key, val);
    } else {
      // Try to combine v to value
      return combiner.combine(curVal, val);
    }
  }

  public V getVal(int key) {
    P partition = getKVPartition(key);
    if (partition != null) {
      return partition.getVal(key);
    } else {
      return null;
    }
  }

  public V removeVal(int key) {
    P partition = getKVPartition(key);
    if (partition != null) {
      return partition.removeVal(key);
    } else {
      return null;
    }
  }

  private P getOrCreateKVPartition(int key) {
    int partitionID = getKVPartitionID(key);
    P partition = this.getPartition(partitionID);
    if (partition == null) {
      partition =
        this.getResourcePool().getWritableObject(
          pClass);
      // this.getResourcePool().createObject(
      // pClass);
      partition.initialize(partitionID,
        expectedKVCountPerPartition, vClass);
      this.insertPartition(partition);
    }
    return partition;
  }

  private P getKVPartition(int key) {
    int partitionID = getKVPartitionID(key);
    return this.getPartition(partitionID);
  }

  /**
   * Return the partition ID of a given key
   * 
   * @param key
   * @return
   */
  protected int getKVPartitionID(int key) {
    return key % this.partitionSeed;
  }

  protected int getPartitionSeed() {
    return this.partitionSeed;
  }

  @Override
  protected boolean checkIfPartitionAddable(P p) {
    return true;
  }

  @Override
  protected PartitionStatus combinePartition(
    P op, P np) {
    // remove method in iterator has a bug
    // the rest keys may not be traversed if
    // the last key was removed because the
    // position of keys could be shifted.
    Int2ObjectOpenHashMap<V> nMap = np.getKVMap();
    ObjectIterator<Int2ObjectMap.Entry<V>> iterator =
      nMap.int2ObjectEntrySet().fastIterator();
    IntArrayList rmKeys = new IntArrayList();
    while (iterator.hasNext()) {
      Int2ObjectMap.Entry<V> entry =
        iterator.next();
      int key = entry.getIntKey();
      KeyValStatus status =
        addKVInPartition(op, key,
          entry.getValue());
      if (status == KeyValStatus.ADDED) {
        rmKeys.add(key);
      }
    }
    for (int rmKey : rmKeys) {
      nMap.remove(rmKey);
    }
    return PartitionStatus.COMBINED;
  }
}
