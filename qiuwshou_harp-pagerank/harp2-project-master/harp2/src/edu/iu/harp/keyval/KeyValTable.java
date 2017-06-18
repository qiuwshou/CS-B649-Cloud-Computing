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

package edu.iu.harp.keyval;

import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.iu.harp.partition.PartitionStatus;
import edu.iu.harp.partition.StructTable;
import edu.iu.harp.resource.ResourcePool;

public class KeyValTable<K extends Key, V extends Value>
  extends StructTable<KeyValPartition> {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(KeyValTable.class);

  private final int partitionSeed;
  private final Class<K> kClass;
  private final Class<V> vClass;
  private final ValCombiner<V> combiner;

  public KeyValTable(Class<K> kClass,
    Class<V> vClass, ValCombiner<V> combiner,
    int partitionSeed, ResourcePool pool) {
    super(pool);
    this.kClass = kClass;
    this.vClass = vClass;
    this.combiner = combiner;
    this.partitionSeed = partitionSeed;
  }

  public Class<K> getKClass() {
    return this.kClass;
  }

  public Class<V> getVClass() {
    return this.vClass;
  }

  public int getPartitionSeed() {
    return partitionSeed;
  }

  /**
   * Key and value are not created through the
   * resource pool but through "new"
   * 
   * @param k
   * @param v
   */
  public KeyValStatus addKeyVal(K k, V v) {
    int hashcode = Math.abs(k.hashCode());
    int partitionID = hashcode % partitionSeed;
    KeyValPartition partition =
      getPartition(partitionID);
    if (partition == null) {
      partition =
        this.getResourcePool().getWritableObject(
          KeyValPartition.class);
      partition.initialize(partitionID, kClass,
        vClass);
      insertPartition(partition);
    }
    return addKeyValInPartition(partition, k, v);
  }

  public V getVal(K k) {
    int hashcode = Math.abs(k.hashCode());
    int partitionID = hashcode % partitionSeed;
    KeyValPartition partition =
      getPartition(partitionID);
    if (partition == null) {
      return null;
    } else {
      return (V) partition.getVal(k);
    }
  }

  private KeyValStatus addKeyValInPartition(
    KeyValPartition partition, K k, V v) {
    V val = (V) partition.getVal(k);
    if (val == null) {
      partition.putKeyVal(k, v);
      return KeyValStatus.ADDED;
    } else {
      // Try to combine v to value
      return combiner.combine(val, v);
    }
  }

  @Override
  protected boolean checkIfPartitionAddable(
    KeyValPartition p) {
    // partition seed checking is required in
    // future
    return true;
  }

  @Override
  protected PartitionStatus combinePartition(
    KeyValPartition curP, KeyValPartition newP) {
    // Remove the key-values added to the current
    // partition from the new partition,
    // this will break the integrity the original
    // partition, but then we can release it
    LinkedList<Key> rmKeys = new LinkedList<>();
    for (Entry<Key, Value> entry : newP
      .getKeyVals()) {
      K key = (K) entry.getKey();
      V value = (V) entry.getValue();
      KeyValStatus status =
        addKeyValInPartition(curP, key, value);
      if (status == KeyValStatus.ADDED) {
        rmKeys.add(key);
      }
    }
    for (Key key : rmKeys) {
      newP.removeVal(key);
    }
    return PartitionStatus.COMBINED;
  }
}
