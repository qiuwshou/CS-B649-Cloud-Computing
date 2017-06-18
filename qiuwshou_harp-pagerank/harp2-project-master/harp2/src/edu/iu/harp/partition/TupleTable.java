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

import java.util.List;

import org.apache.log4j.Logger;

import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.ByteArray;
import edu.iu.harp.trans.StructObject;

public abstract class TupleTable extends
  Table<TuplePartition> {

  protected static final Logger LOG = Logger
    .getLogger(TupleTable.class);

  private final Class<StructObject>[] tupleClass;
  private final int byteArraySize;
  private final int partitionSeed;
  private final ResourcePool pool;

  public TupleTable(
    Class<StructObject>[] classes,
    int partitionSeed, ResourcePool pool) {
    super();
    this.tupleClass = classes;
    this.byteArraySize =
      PartitionConstants.DEFAULT_BYTE_ARRAY_SIZE;
    this.partitionSeed = partitionSeed;
    this.pool = pool;
  }

  public TupleTable(
    Class<StructObject>[] classes,
    int byteArrSize, int partitionSeed,
    ResourcePool pool) {
    super();
    this.tupleClass = classes;
    this.byteArraySize = byteArrSize;
    this.partitionSeed = partitionSeed;
    this.pool = pool;
  }

  protected int getPartitionSeed() {
    return this.partitionSeed;
  }

  public Class<StructObject>[] getTupleclass() {
    return tupleClass;
  }

  protected boolean
    addTuple(StructObject[] tuple) {
    int partitionID = getPartitionID(tuple);
    // LOG.info("Partiiton ID: " + partitionID);
    TuplePartition partition =
      getOrCreateTuplePartition(partitionID);
    boolean isSuccess =
      partition.writeTuple(tuple);
    if (!isSuccess) {
      if (!partition.hasSpaceToWrite()) {
        // LOG.info("Add byte array.");
        byte[] bytes =
          pool.getBytes(byteArraySize);
        partition.addByteArray(new ByteArray(
          bytes, 0, bytes.length), true);
        return partition.writeTuple(tuple);
      }
      return false;
    } else {
      return true;
    }
  }

  private TuplePartition
    getOrCreateTuplePartition(int partitionID) {
    TuplePartition partition =
      getPartition(partitionID);
    if (partition == null) {
      partition =
        new TuplePartition(partitionID,
          tupleClass);
      this.getPartitionMap().put(partitionID,
        partition);
    }
    return partition;
  }

  public void releaseTuplePartition(
    int partitionID) {
    TuplePartition partition =
      this.removePartition(partitionID);
    List<ByteArray> arrays =
      partition.removeByteArrays();
    for (ByteArray array : arrays) {
      pool.releaseBytes(array.getArray());
    }
  }

  public void clear() {
    for (int partitionID : this.getPartitionIDs()) {
      releaseTuplePartition(partitionID);
    }
    this.getPartitionMap().clear();
  }

  @Override
  protected boolean checkIfPartitionAddable(
    TuplePartition p) {
    if (!p.hasTupleClass()) {
      return false;
    } else {
      Class<StructObject>[] pTuplePclass =
        p.getTupleClass();
      if (tupleClass.length != pTuplePclass.length) {
        return false;
      } else {
        for (int i = 0; i < tupleClass.length; i++) {
          if (!tupleClass[i]
            .equals(pTuplePclass[i])) {
            return false;
          }
        }
        return true;
      }
    }
  }

  @Override
  protected PartitionStatus combinePartition(
    TuplePartition curP, TuplePartition newP) {
    newP.finalizeByteArrays(pool);
    List<ByteArray> rmArrays =
      newP.removeByteArrays();
    for (ByteArray array : rmArrays) {
      curP.addByteArray(array, false);
    }
    // This partition is added but not merged
    return PartitionStatus.ADDED;
  }

  public abstract int getPartitionID(
    StructObject[] tuple);
}
