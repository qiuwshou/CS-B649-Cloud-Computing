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

import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.util.LinkedList;

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
public class Key2ValKVTable<K extends ObjectK, V extends ObjectV, P extends Key2ValKVPartition<K, V>>
  extends StructTable<P> {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(Key2ValKVTable.class);

  private final Class<K> kClass;
  private final Class<V> vClass;
  private final Class<P> pClass;
  private final ValCombiner<V> combiner;
  private final int expectedKVCount;
  private final int partitionSeed;

  public Key2ValKVTable(Class<K> kClass,
    Class<V> vClass, Class<P> pClass,
    ValCombiner<V> combiner,
    int expectedKVPerPartition,
    int partitionSeed, ResourcePool pool) {
    super(pool);
    this.kClass = kClass;
    this.vClass = vClass;
    this.pClass = pClass;
    this.combiner = combiner;
    this.expectedKVCount = expectedKVPerPartition;
    this.partitionSeed = partitionSeed;
  }

  public KeyValStatus addKeyVal(K key, V val) {
    P partition = getOrCreateKVPartition(key);
    return addKVInPartition(partition, key, val);
  }

  private KeyValStatus addKVInPartition(
    P partition, K key, V val) {
    V curVal = partition.getVal(key);
    if (curVal == null) {
      return partition.putKeyVal(key, val);
    } else {
      // Try to combine v to value
      return combiner.combine(curVal, val);
    }
  }

  public V getVal(K key) {
    P partition = getKVPartition(key);
    if (partition != null) {
      return partition.getVal(key);
    } else {
      return null;
    }
  }

  public V removeVal(K key) {
    P partition = getKVPartition(key);
    if (partition != null) {
      return partition.removeVal(key);
    } else {
      return null;
    }
  }

  private P getOrCreateKVPartition(K key) {
    int partitionID = getKVPartitionID(key);
    P partition = this.getPartition(partitionID);
    if (partition == null) {
      partition =
        this.getResourcePool().getWritableObject(
          pClass);
      partition.initialize(partitionID,
        expectedKVCount, kClass, vClass);
      this.insertPartition(partition);
    }
    return partition;
  }

  private P getKVPartition(K key) {
    int partitionID = getKVPartitionID(key);
    return this.getPartition(partitionID);
  }

  private int getKVPartitionID(K key) {
    return Math.abs(key.hashCode())
      % this.partitionSeed;
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
    Object2ObjectOpenHashMap<K, V> nMap =
      np.getKVMap();
    LinkedList<K> rmKeys = new LinkedList<>();
    K key = null;
    V val = null;
    KeyValStatus status = null;
    for (Object2ObjectMap.Entry<K, V> entry : nMap
      .object2ObjectEntrySet()) {
      key = entry.getKey();
      val = entry.getValue();
      status = addKVInPartition(op, key, val);
      if (status == KeyValStatus.ADDED) {
        rmKeys.add(key);
      }
    }
    for (K rmKey : rmKeys) {
      nMap.remove(rmKey);
    }
    return PartitionStatus.COMBINED;
  }
}
