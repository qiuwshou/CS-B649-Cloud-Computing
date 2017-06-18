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

package edu.iu.harp.partition;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.Collection;
import java.util.Set;

public abstract class Table<P extends Partition> {

  private final Int2ObjectOpenHashMap<P> partitions;

  public Table() {
    this.partitions =
      new Int2ObjectOpenHashMap<>();
  }

  public final int getNumPartitions() {
    return partitions.size();
  }

  public final Set<Integer> getPartitionIDs() {
    return partitions.keySet();
  }

  public final Collection<P> getPartitions() {
    return partitions.values();
  }

  public final Int2ObjectOpenHashMap<P>
    getPartitionMap() {
    return this.partitions;
  }

  /**
   * Add a partition into a table
   * 
   * @param partition
   * @return
   */
  public final PartitionStatus addPartition(
    P partition) {
    if (!checkIfPartitionAddable(partition)) {
      return PartitionStatus.ADD_FAILED;
    }
    P curPartition =
      this.partitions.get(partition
        .getPartitionID());
    if (curPartition == null) {
      return insertPartition(partition);
    } else {
      // Try to merge two partitions
      return combinePartition(curPartition,
        partition);
    }
  }

  protected abstract boolean
    checkIfPartitionAddable(P p);

  protected final PartitionStatus
    insertPartition(P partition) {
    partitions.put(partition.getPartitionID(),
      partition);
    return PartitionStatus.ADDED;
  }

  /**
   * Merge the new partition to the current
   * partition
   * 
   * @param curP
   * @param newP
   * @return
   */
  protected abstract PartitionStatus
    combinePartition(P curP, P newP);

  public final P getPartition(int partitionID) {
    return partitions.get(partitionID);
  }

  public final P removePartition(int partitionID) {
    return partitions.remove(partitionID);
  }

  public final boolean isEmpty() {
    return partitions.isEmpty();
  }
}
