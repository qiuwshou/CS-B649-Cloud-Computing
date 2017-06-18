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

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.iu.harp.array.ArrPartition;
import edu.iu.harp.array.ArrTable;
import edu.iu.harp.comm.Communication;
import edu.iu.harp.compute.Computation;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.DataStatus;
import edu.iu.harp.io.DataType;
import edu.iu.harp.io.DataUtils;
import edu.iu.harp.io.Deserializer;
import edu.iu.harp.io.IOUtils;
import edu.iu.harp.io.Serializer;
import edu.iu.harp.message.PartitionCount;
import edu.iu.harp.message.PartitionDistr;
import edu.iu.harp.message.PartitionSet;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.Array;
import edu.iu.harp.trans.ByteArray;
import edu.iu.harp.trans.DoubleArray;
import edu.iu.harp.trans.IntArray;
import edu.iu.harp.worker.Workers;

public class PartitionUtils {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(PartitionUtils.class);

  public static <P extends Partition> int
    getPartitionCountInSending(Table<P> table,
      ResourcePool pool) {
    if (table instanceof TupleTable) {
      TupleTable tupleTable = (TupleTable) table;
      int partitionCount = 0;
      for (TuplePartition partition : tupleTable
        .getPartitions()) {
        partition.finalizeByteArrays(pool);
        partitionCount +=
          partition.getNumByteArrays();
      }
      return partitionCount;
    } else {
      return table.getNumPartitions();
    }
  }

  public static <P extends Partition>
    Int2IntOpenHashMap getPartitionDistribution(
      Table<P> table, ResourcePool pool) {
    Int2IntOpenHashMap parIDCount =
      new Int2IntOpenHashMap();
    parIDCount.defaultReturnValue(0);
    if (table instanceof TupleTable) {
      TupleTable tupleTable = (TupleTable) table;
      for (TuplePartition partition : tupleTable
        .getPartitions()) {
        partition.finalizeByteArrays(pool);
        parIDCount.put(
          partition.getPartitionID(),
          partition.getNumByteArrays());
      }
    } else {
      for (int partitionID : table
        .getPartitionIDs()) {
        parIDCount.put(partitionID, 1);
      }
    }
    return parIDCount;
  }

  public static
    Int2IntOpenHashMap
    getPartitionDistribution(TableType tableType,
      Partition[] partitionList, ResourcePool pool) {
    Int2IntOpenHashMap parIDCount =
      new Int2IntOpenHashMap();
    parIDCount.defaultReturnValue(0);
    if (tableType == TableType.TUPLE_TABLE) {
      for (Partition par : partitionList) {
        TuplePartition partition =
          (TuplePartition) par;
        partition.finalizeByteArrays(pool);
        parIDCount.put(
          partition.getPartitionID(),
          partition.getNumByteArrays());
      }
    } else {
      for (Partition par : partitionList) {
        parIDCount.put(par.getPartitionID(), 1);
      }
    }
    return parIDCount;
  }

  public static IntArrayList getPartitionSet(
    Partition[] partitionList) {
    IntArrayList parIDCount =
      new IntArrayList(partitionList.length);
    for (Partition par : partitionList) {
      parIDCount.add(par.getPartitionID());
    }
    return parIDCount;
  }

  public static
    <T, A extends Array<T>, P extends Partition>
    boolean sendPartition(String contextName,
      int sourceWorkerID, String operationName,
      P partition, int destWorkerID,
      Workers workers, ResourcePool resourcePool,
      boolean decode) {
    if (partition instanceof StructPartition) {
      return Communication.send(contextName,
        sourceWorkerID, operationName,
        partition.getPartitionID(),
        (StructPartition) partition, true, true,
        destWorkerID, workers, resourcePool,
        decode);
    } else if (partition instanceof ArrPartition) {
      ArrPartition<A> arrPartition =
        (ArrPartition<A>) partition;
      return Communication.send(contextName,
        sourceWorkerID, operationName,
        partition.getPartitionID(),
        arrPartition.getArray(), true, true,
        destWorkerID, workers, resourcePool,
        decode);
    } else if (partition instanceof TuplePartition) {
      TuplePartition tuplePartition =
        (TuplePartition) partition;
      tuplePartition
        .finalizeByteArrays(resourcePool);
      int partitionID =
        tuplePartition.getPartitionID();
      boolean isSuccess = false;
      for (ByteArray array : tuplePartition
        .getByteArrays()) {
        isSuccess =
          Communication.send(contextName,
            sourceWorkerID, operationName,
            partitionID, array, true, true,
            destWorkerID, workers, resourcePool,
            decode);
        if (!isSuccess) {
          break;
        }
      }
      return isSuccess;
    } else {
      return false;
    }
  }

  public static
    <T, A extends Array<T>, P extends Partition>
    boolean multicastPartition(
      String contextName, int sourceWorkerID,
      String operationName, P partition,
      List<Integer> destWorkerIDs,
      Workers workers, ResourcePool resourcePool,
      boolean decode) {
    if (partition instanceof StructPartition) {
      return Communication.multicast(contextName,
        sourceWorkerID, operationName,
        partition.getPartitionID(),
        (StructPartition) partition, true, true,
        destWorkerIDs, workers, resourcePool,
        decode);
    } else if (partition instanceof ArrPartition) {
      ArrPartition<A> arrPartition =
        (ArrPartition<A>) partition;
      return Communication.multicast(contextName,
        sourceWorkerID, operationName,
        partition.getPartitionID(),
        arrPartition.getArray(), true, true,
        destWorkerIDs, workers, resourcePool,
        decode);
    } else if (partition instanceof TuplePartition) {
      TuplePartition tuplePartition =
        (TuplePartition) partition;
      tuplePartition
        .finalizeByteArrays(resourcePool);
      int partitionID =
        tuplePartition.getPartitionID();
      boolean isSuccess = false;
      for (ByteArray array : tuplePartition
        .getByteArrays()) {
        isSuccess =
          Communication.multicast(contextName,
            sourceWorkerID, operationName,
            partitionID, array, true, true,
            destWorkerIDs, workers, resourcePool,
            decode);
        if (!isSuccess) {
          break;
        }
      }
      return isSuccess;
    } else {
      return false;
    }
  }

  public static
    <T, A extends Array<T>, P extends Partition>
    boolean multicastOrCopyPartition(
      String contextName, int sourceWorkerID,
      String operationName, P partition,
      List<Integer> destWorkerIDs,
      DataMap dataMap, Workers workers,
      ResourcePool resourcePool, boolean decode) {
    if (partition instanceof StructPartition) {
      return Communication.multicastOrCopy(
        contextName, sourceWorkerID,
        operationName,
        partition.getPartitionID(),
        (StructPartition) partition, true, true,
        destWorkerIDs, dataMap, workers,
        resourcePool, decode);
    } else if (partition instanceof ArrPartition) {
      ArrPartition<A> arrPartition =
        (ArrPartition<A>) partition;
      return Communication.multicastOrCopy(
        contextName, sourceWorkerID,
        operationName,
        partition.getPartitionID(),
        arrPartition.getArray(), true, true,
        destWorkerIDs, dataMap, workers,
        resourcePool, decode);
    } else if (partition instanceof TuplePartition) {
      TuplePartition tuplePartition =
        (TuplePartition) partition;
      tuplePartition
        .finalizeByteArrays(resourcePool);
      int partitionID =
        tuplePartition.getPartitionID();
      boolean isSuccess = false;
      for (ByteArray array : tuplePartition
        .getByteArrays()) {
        isSuccess =
          Communication.multicastOrCopy(
            contextName, sourceWorkerID,
            operationName, partitionID, array,
            true, true, destWorkerIDs, dataMap,
            workers, resourcePool, decode);
        if (!isSuccess) {
          break;
        }
      }
      return isSuccess;
    } else {
      return false;
    }
  }

  public static boolean copyPartitions(
    List<Partition> partitions,
    List<Partition> copiedPartitions,
    ResourcePool resourcePool) {
    Data data =
      new Data(DataType.PARTITION_LIST, "",
        Constants.UNKNOWN_WORKER_ID, "",
        partitions.size(), partitions);
    data.encodeHead(resourcePool);
    data.encodeBody(resourcePool);
    Data newData =
      new Data(data.getHeadArray(),
        data.getBodyArray());
    data = null;
    newData.decodeHeadArray(resourcePool);
    newData.releaseHeadArray(resourcePool);
    newData.decodeBodyArray(resourcePool);
    newData.releaseBodyArray(resourcePool);
    if ((newData.getHeadStatus() == DataStatus.DECODED && newData
      .getBodyStatus() == DataStatus.DECODED)
      || (newData.getHeadStatus() == DataStatus.ENCODED_ARRAY_DECODED && newData
        .getBodyStatus() == DataStatus.ENCODED_ARRAY)
      && newData.isOperationData()) {
      copiedPartitions
        .addAll((LinkedList<Partition>) newData
          .getBody());
      return true;
    } else {
      return false;
    }
  }

  public static boolean
    copyPartitionsInMultiTasks(
      Partition[] partitions,
      Partition[] copiedPartitions,
      PartitionType partitionType, int numTasks,
      ResourcePool resourcePool) {
    Computation<PartitionCopyTask> compute = null;
    LinkedList<Partition> partitionList =
      new LinkedList<>();
    int partitionSize = 1;
    int partitionCount = 0;
    int numPartitions = partitions.length;
    int copyCount = 0;
    boolean isFailed = false;
    int newPartitionCount = 0;
    for (Partition partition : partitions) {
      partitionCount++;
      partitionList.add(partition);
      partitionSize +=
        PartitionUtils.getPartitionSizeInBytes(
          partitionType, partition, resourcePool);
      // If more than 100 MB
      if (partitionSize > Constants.MAX_PARTITION_SEND_BYTE_SIZE
        || partitionCount == numPartitions) {
        copyCount++;
        Data data =
          new Data(DataType.PARTITION_LIST, "",
            Constants.UNKNOWN_WORKER_ID, "",
            partitionList.size(), partitionList);
        if (copyCount == 1
          && partitionCount == numPartitions) {
          data.encodeHead(resourcePool);
          data.encodeBody(resourcePool);
          Data newData =
            new Data(data.getHeadArray(),
              data.getBodyArray());
          newData.decodeHeadArray(resourcePool);
          newData.releaseHeadArray(resourcePool);
          newData.decodeBodyArray(resourcePool);
          newData.releaseBodyArray(resourcePool);
          if ((newData.getHeadStatus() == DataStatus.DECODED && newData
            .getBodyStatus() == DataStatus.DECODED)
            && newData.isOperationData()) {
            LinkedList<Partition> newPartitionList =
              (LinkedList<Partition>) newData
                .getBody();
            for (Partition newPartition : newPartitionList) {
              copiedPartitions[newPartitionCount++] =
                newPartition;
            }
          }
        } else {
          if (copyCount == 1) {
            List<PartitionCopyTask> tasks =
              new LinkedList<>();
            for (int i = 0; i < numTasks; i++) {
              tasks.add(new PartitionCopyTask(
                resourcePool));
            }
            compute = new Computation<>(tasks);
            compute.start();
          }
          compute.submit(data);
          partitionList = new LinkedList<>();
          partitionSize = 1;
        }
      }
    }
    if (copyCount > 1) {
      compute.stop();
      Object output = null;
      do {
        output = compute.waitOutput();
        if (output != null
          && output instanceof Data) {
          Data data = (Data) output;
          if ((data.getHeadStatus() == DataStatus.DECODED && data
            .getBodyStatus() == DataStatus.DECODED)
            && data.isOperationData()) {
            LinkedList<Partition> newPartitionList =
              (LinkedList<Partition>) data
                .getBody();
            for (Partition newPartition : newPartitionList) {
              copiedPartitions[newPartitionCount++] =
                newPartition;
            }
          } else {
            isFailed = true;
          }
        }
      } while (output != null);
    }
    return !isFailed;
  }

  public static
    <T, A extends Array<T>, P extends Partition>
    boolean chainBcastPartition(
      String contextName, int bcastWorkerID,
      String operationName, P partition,
      Workers workers, ResourcePool resourcePool,
      boolean decode) {
    if (partition instanceof StructPartition) {
      return Communication.chainBcast(
        contextName, bcastWorkerID,
        operationName,
        partition.getPartitionID(),
        (StructPartition) partition, true, true,
        workers, resourcePool, decode);
    } else if (partition instanceof ArrPartition) {
      ArrPartition<A> arrPartition =
        (ArrPartition<A>) partition;
      return Communication.chainBcast(
        contextName, bcastWorkerID,
        operationName,
        partition.getPartitionID(),
        arrPartition.getArray(), true, true,
        workers, resourcePool, decode);
    } else if (partition instanceof TuplePartition) {
      TuplePartition tuplePartition =
        (TuplePartition) partition;
      tuplePartition
        .finalizeByteArrays(resourcePool);
      int partitionID =
        tuplePartition.getPartitionID();
      boolean isSuccess = false;
      for (ByteArray array : tuplePartition
        .getByteArrays()) {
        isSuccess =
          Communication.chainBcast(contextName,
            bcastWorkerID, operationName,
            partitionID, array, true, true,
            workers, resourcePool, decode);
        if (!isSuccess) {
          break;
        }
      }
      return isSuccess;
    } else {
      return false;
    }
  }

  public static
    <T, A extends Array<T>, P extends Partition>
    void releasePartition(ResourcePool pool,
      P partition) {
    if (partition instanceof StructPartition) {
      pool
        .releaseWritableObject((StructPartition) partition);
    } else if (partition instanceof ArrPartition) {
      A array =
        ((ArrPartition<A>) partition).getArray();
      if (array instanceof ByteArray) {
        pool.releaseBytes((byte[]) array
          .getArray());
      } else if (array instanceof IntArray) {
        pool
          .releaseInts((int[]) array.getArray());
      } else if (array instanceof DoubleArray) {
        pool.releaseDoubles((double[]) array
          .getArray());
      }
    } else if (partition instanceof TuplePartition) {
      TuplePartition tuplePartition =
        (TuplePartition) partition;
      tuplePartition.finalizeByteArrays(pool);
      for (ByteArray byteArray : tuplePartition
        .removeByteArrays()) {
        pool.releaseBytes(byteArray.getArray());
      }
    }
  }

  public static
    <T, A extends Array<T>, P extends Partition>
    void releasePartition(ResourcePool pool,
      P partition, PartitionType partitionType) {
    if (partitionType == PartitionType.STRUCT_PARTITION) {
      pool
        .releaseWritableObject((StructPartition) partition);
    } else if (partitionType == PartitionType.BYTE_PARTITION) {
      A array =
        ((ArrPartition<A>) partition).getArray();
      pool
        .releaseBytes((byte[]) array.getArray());
    } else if (partitionType == PartitionType.INT_PARTITION) {
      A array =
        ((ArrPartition<A>) partition).getArray();
      pool.releaseInts((int[]) array.getArray());
    } else if (partitionType == PartitionType.DOUBLE_PARTITION) {
      A array =
        ((ArrPartition<A>) partition).getArray();
      pool.releaseDoubles((double[]) array
        .getArray());
    } else if (partitionType == PartitionType.TUPLE_PARTITION) {
      TuplePartition tuplePartition =
        (TuplePartition) partition;
      tuplePartition.finalizeByteArrays(pool);
      for (ByteArray byteArray : tuplePartition
        .removeByteArrays()) {
        pool.releaseBytes(byteArray.getArray());
      }
    }
  }

  public static
    <T, A extends Array<T>, P extends Partition>
    void releaseTable(ResourcePool resourcePool,
      Table<P> table) {
    Int2ObjectOpenHashMap<P> partitionMap =
      table.getPartitionMap();
    ObjectIterator<Int2ObjectMap.Entry<P>> iterator =
      partitionMap.int2ObjectEntrySet()
        .fastIterator();
    // Release the first partition
    // Check partition type
    PartitionType partitionType =
      PartitionType.UNKNOWN_PARTITION;
    if (iterator.hasNext()) {
      Int2ObjectMap.Entry<P> entry =
        iterator.next();
      P partition = entry.getValue();
      if (partition instanceof StructPartition) {
        partitionType =
          PartitionType.STRUCT_PARTITION;
        resourcePool
          .releaseWritableObject((StructPartition) partition);
      } else if (partition.getClass().equals(
        ArrPartition.class)) {
        A array =
          ((ArrPartition<A>) partition)
            .getArray();
        Class<?> arrayClass = array.getClass();
        if (arrayClass.equals(ByteArray.class)) {
          partitionType =
            PartitionType.BYTE_PARTITION;
          resourcePool
            .releaseBytes((byte[]) array
              .getArray());
        } else if (arrayClass
          .equals(IntArray.class)) {
          partitionType =
            PartitionType.INT_PARTITION;
          resourcePool.releaseInts((int[]) array
            .getArray());
        } else if (arrayClass
          .equals(DoubleArray.class)) {
          partitionType =
            PartitionType.DOUBLE_PARTITION;
          resourcePool
            .releaseDoubles((double[]) array
              .getArray());
        }
      } else if (partition.getClass().equals(
        TuplePartition.class)) {
        partitionType =
          PartitionType.TUPLE_PARTITION;
        TuplePartition tuplePartition =
          (TuplePartition) partition;
        tuplePartition
          .finalizeByteArrays(resourcePool);
        for (ByteArray byteArray : tuplePartition
          .removeByteArrays()) {
          resourcePool.releaseBytes(byteArray
            .getArray());
        }
      }
    }
    while (iterator.hasNext()) {
      Int2ObjectMap.Entry<P> entry =
        iterator.next();
      P partition = entry.getValue();
      if (partitionType == PartitionType.STRUCT_PARTITION) {
        resourcePool
          .releaseWritableObject((StructPartition) partition);
      } else if (partitionType == PartitionType.BYTE_PARTITION) {
        A array =
          ((ArrPartition<A>) partition)
            .getArray();
        resourcePool.releaseBytes((byte[]) array
          .getArray());
      } else if (partitionType == PartitionType.INT_PARTITION) {
        A array =
          ((ArrPartition<A>) partition)
            .getArray();
        resourcePool.releaseInts((int[]) array
          .getArray());
      } else if (partitionType == PartitionType.DOUBLE_PARTITION) {
        A array =
          ((ArrPartition<A>) partition)
            .getArray();
        resourcePool
          .releaseDoubles((double[]) array
            .getArray());
      } else if (partitionType == PartitionType.TUPLE_PARTITION) {
        TuplePartition tuplePartition =
          (TuplePartition) partition;
        tuplePartition
          .finalizeByteArrays(resourcePool);
        for (ByteArray byteArray : tuplePartition
          .removeByteArrays()) {
          resourcePool.releaseBytes(byteArray
            .getArray());
        }
      }
    }
    partitionMap.clear();
  }

  public static
    <T, A extends Array<T>, P extends Partition>
    void
    releaseTable(ResourcePool resourcePool,
      Table<P> table, PartitionType partitionType) {
    Int2ObjectOpenHashMap<P> partitionMap =
      table.getPartitionMap();
    if (partitionType == PartitionType.STRUCT_PARTITION) {
      for (P partition : partitionMap.values()) {
        resourcePool
          .releaseWritableObject((StructPartition) partition);
      }
    } else if (partitionType == PartitionType.BYTE_PARTITION) {
      for (P partition : partitionMap.values()) {
        A array =
          ((ArrPartition<A>) partition)
            .getArray();
        resourcePool.releaseBytes((byte[]) array
          .getArray());
      }
    } else if (partitionType == PartitionType.INT_PARTITION) {
      for (P partition : partitionMap.values()) {
        A array =
          ((ArrPartition<A>) partition)
            .getArray();
        resourcePool.releaseInts((int[]) array
          .getArray());
      }
    } else if (partitionType == PartitionType.DOUBLE_PARTITION) {
      for (P partition : partitionMap.values()) {
        A array =
          ((ArrPartition<A>) partition)
            .getArray();
        resourcePool
          .releaseDoubles((double[]) array
            .getArray());
      }
    } else if (partitionType == PartitionType.TUPLE_PARTITION) {
      for (P partition : partitionMap.values()) {
        TuplePartition tuplePartition =
          (TuplePartition) partition;
        tuplePartition
          .finalizeByteArrays(resourcePool);
        for (ByteArray byteArray : tuplePartition
          .removeByteArrays()) {
          resourcePool.releaseBytes(byteArray
            .getArray());
        }
      }
    }
    partitionMap.clear();
  }

  public static <T, A extends Array<T>> void
    releasePartitions(ResourcePool resourcePool,
      Partition[] partitionList,
      PartitionType partitionType) {
    if (partitionType == PartitionType.STRUCT_PARTITION) {
      for (Partition partition : partitionList) {
        resourcePool
          .releaseWritableObject((StructPartition) partition);
      }
    } else if (partitionType == PartitionType.BYTE_PARTITION) {
      for (Partition partition : partitionList) {
        A array =
          ((ArrPartition<A>) partition)
            .getArray();
        resourcePool.releaseBytes((byte[]) array
          .getArray());
      }
    } else if (partitionType == PartitionType.INT_PARTITION) {
      for (Partition partition : partitionList) {
        A array =
          ((ArrPartition<A>) partition)
            .getArray();
        resourcePool.releaseInts((int[]) array
          .getArray());
      }
    } else if (partitionType == PartitionType.DOUBLE_PARTITION) {
      for (Partition partition : partitionList) {
        A array =
          ((ArrPartition<A>) partition)
            .getArray();
        resourcePool
          .releaseDoubles((double[]) array
            .getArray());
      }
    } else if (partitionType == PartitionType.TUPLE_PARTITION) {
      for (Partition partition : partitionList) {
        TuplePartition tuplePartition =
          (TuplePartition) partition;
        tuplePartition
          .finalizeByteArrays(resourcePool);
        for (ByteArray byteArray : tuplePartition
          .removeByteArrays()) {
          resourcePool.releaseBytes(byteArray
            .getArray());
        }
      }
    }
  }

  public static <T, A extends Array<T>> void
    releasePartitions(ResourcePool resourcePool,
      LinkedList<Partition> partitionList,
      PartitionType partitionType) {
    if (partitionType == PartitionType.STRUCT_PARTITION) {
      for (Partition partition : partitionList) {
        resourcePool
          .releaseWritableObject((StructPartition) partition);
      }
    } else if (partitionType == PartitionType.BYTE_PARTITION) {
      for (Partition partition : partitionList) {
        A array =
          ((ArrPartition<A>) partition)
            .getArray();
        resourcePool.releaseBytes((byte[]) array
          .getArray());
      }
    } else if (partitionType == PartitionType.INT_PARTITION) {
      for (Partition partition : partitionList) {
        A array =
          ((ArrPartition<A>) partition)
            .getArray();
        resourcePool.releaseInts((int[]) array
          .getArray());
      }
    } else if (partitionType == PartitionType.DOUBLE_PARTITION) {
      for (Partition partition : partitionList) {
        A array =
          ((ArrPartition<A>) partition)
            .getArray();
        resourcePool
          .releaseDoubles((double[]) array
            .getArray());
      }
    } else if (partitionType == PartitionType.TUPLE_PARTITION) {
      for (Partition partition : partitionList) {
        TuplePartition tuplePartition =
          (TuplePartition) partition;
        tuplePartition
          .finalizeByteArrays(resourcePool);
        for (ByteArray byteArray : tuplePartition
          .removeByteArrays()) {
          resourcePool.releaseBytes(byteArray
            .getArray());
        }
      }
    }
  }

  public static <P extends Partition> void
    addPartitionToTable(Partition partition,
      Table<P> table, ResourcePool resourcePool) {
    PartitionStatus status = null;
    if (table instanceof TupleTable
      && partition.getClass().equals(
        ArrPartition.class)) {
      // Convert ArrPartition<ByteArray> to
      // TuplePartition
      TupleTable tupleTable = (TupleTable) table;
      ArrPartition<ByteArray> arrPartition =
        (ArrPartition<ByteArray>) partition;
      TuplePartition tuplePartition =
        new TuplePartition(
          arrPartition.getPartitionID(),
          tupleTable.getTupleclass());
      tuplePartition.addByteArray(
        arrPartition.getArray(), false);
      status =
        tupleTable.addPartition(tuplePartition);
    } else {
      status = table.addPartition((P) partition);
    }
    // LOG
    // .info("Add a partition to table - partition ID: "
    // + partition.getPartitionID()
    // + ", status: " + status);
    if (status != PartitionStatus.ADDED) {
      releasePartition(resourcePool, partition);
    }
  }

  public static <P extends Partition> void
    addPartitionToTable(Partition partition,
      PartitionType partitionType,
      Table<P> table, TableType tableType,
      ResourcePool resourcePool) {
    PartitionStatus status = null;
    if (tableType == TableType.TUPLE_TABLE
      && partitionType == PartitionType.BYTE_PARTITION) {
      // Convert ArrPartition<ByteArray> to
      // TuplePartition
      TupleTable tupleTable = (TupleTable) table;
      ArrPartition<ByteArray> arrPartition =
        (ArrPartition<ByteArray>) partition;
      TuplePartition tuplePartition =
        new TuplePartition(
          arrPartition.getPartitionID(),
          tupleTable.getTupleclass());
      tuplePartition.addByteArray(
        arrPartition.getArray(), false);
      status =
        tupleTable.addPartition(tuplePartition);
    } else {
      status = table.addPartition((P) partition);
    }
    // LOG
    // .info("Add a partition to table - partition ID: "
    // + partition.getPartitionID()
    // + ", status: " + status);
    if (status != PartitionStatus.ADDED) {
      releasePartition(resourcePool, partition,
        partitionType);
    }
  }

  public static <P extends Partition> void
    addPartitionsToTable(
      LinkedList<Partition> partitions,
      PartitionType partitionType,
      Table<P> table, TableType tableType,
      ResourcePool resourcePool) {
    if (tableType == TableType.TUPLE_TABLE
      && partitionType == PartitionType.BYTE_PARTITION) {
      TupleTable tupleTable = (TupleTable) table;
      for (Partition partition : partitions) {
        // Convert ArrPartition<ByteArray> to
        // TuplePartition
        ArrPartition<ByteArray> arrPartition =
          (ArrPartition<ByteArray>) partition;
        TuplePartition tuplePartition =
          new TuplePartition(
            arrPartition.getPartitionID(),
            tupleTable.getTupleclass());
        tuplePartition.addByteArray(
          arrPartition.getArray(), false);
        PartitionStatus status =
          tupleTable.addPartition(tuplePartition);
        if (status != PartitionStatus.ADDED) {
          releasePartition(resourcePool,
            partition, partitionType);
        }
      }
    } else {
      for (Partition partition : partitions) {
        PartitionStatus status =
          table.addPartition((P) partition);
        if (status != PartitionStatus.ADDED) {
          releasePartition(resourcePool,
            partition, partitionType);
        }
      }
    }
  }

  public static <P extends Partition> void
    combinePartitionToTable(
      Partition curPartition,
      Partition newPartition, Table<P> table,
      ResourcePool resourcePool) {
    PartitionStatus status = null;
    if (table instanceof TupleTable
      && newPartition instanceof ArrPartition) {
      // Convert ArrPartition<ByteArray> to
      // TuplePartition
      TupleTable tupleTable = (TupleTable) table;
      ArrPartition<ByteArray> arrPartition =
        (ArrPartition<ByteArray>) newPartition;
      TuplePartition tuplePartition =
        new TuplePartition(
          arrPartition.getPartitionID(),
          tupleTable.getTupleclass());
      tuplePartition.addByteArray(
        arrPartition.getArray(), false);
      tupleTable.combinePartition(
        (TuplePartition) curPartition,
        tuplePartition);
    } else {
      table.combinePartition((P) curPartition,
        (P) newPartition);
    }
    releasePartition(resourcePool, newPartition);
  }

  public static <P extends Partition> void
    combinePartitionToTable(
      Partition curPartition,
      Partition newPartition,
      PartitionType partitionType,
      Table<P> table, TableType tableType,
      ResourcePool resourcePool) {
    PartitionStatus status = null;
    if (tableType == TableType.TUPLE_TABLE
      && partitionType == PartitionType.BYTE_PARTITION) {
      // Convert ArrPartition<ByteArray> to
      // TuplePartition
      TupleTable tupleTable = (TupleTable) table;
      ArrPartition<ByteArray> arrPartition =
        (ArrPartition<ByteArray>) newPartition;
      TuplePartition tuplePartition =
        new TuplePartition(
          arrPartition.getPartitionID(),
          tupleTable.getTupleclass());
      tuplePartition.addByteArray(
        arrPartition.getArray(), false);
      tupleTable.combinePartition(
        (TuplePartition) curPartition,
        tuplePartition);
    } else {
      table.combinePartition((P) curPartition,
        (P) newPartition);
    }
    releasePartition(resourcePool, newPartition,
      partitionType);
  }

  public static void addPartitionFromData(
    List<Partition> partitions, Data data) {
    Object body = data.getBody();
    if (body instanceof StructPartition) {
      partitions.add((StructPartition) data
        .getBody());
    } else if (body instanceof ByteArray) {
      partitions.add(new ArrPartition<ByteArray>(
        data.getPartitionID(), (ByteArray) body));
    } else if (body instanceof IntArray) {
      partitions.add(new ArrPartition<IntArray>(
        data.getPartitionID(), (IntArray) body));
    } else if (body instanceof DoubleArray) {
      partitions
        .add(new ArrPartition<DoubleArray>(data
          .getPartitionID(), (DoubleArray) body));
    }
  }

  /**
   * Serialize a list of partitions with the same
   * type. We allow the number of partitions can
   * be 0.
   * 
   * @param partitions
   * @param pool
   * @return
   */
  public static <T, A extends Array<T>> ByteArray
    encodePartitionList(
      List<Partition> partitions,
      ResourcePool pool) {
    // Get the size in bytes
    int size = 1;
    PartitionType type =
      getPartitionType(partitions.get(0));
    for (Partition partition : partitions) {
      size +=
        getPartitionSizeInBytes(type, partition,
          pool);
    }
    // Use resource pool to get a byte array
    byte[] bytes = pool.getBytes(size);
    Serializer serializer =
      new Serializer(
        new ByteArray(bytes, 0, size));
    // Start serializing
    try {
      if (size == 1) {
        serializer
          .writeByte(DataType.UNKNOWN_DATA_TYPE);
      } else {
        if (type == PartitionType.STRUCT_PARTITION) {
          serializer
            .writeByte(DataType.STRUCT_OBJECT);
          for (Partition partition : partitions) {
            DataUtils.serializeStructObject(
              ((StructPartition) partition),
              serializer);
          }
        } else if (type == PartitionType.BYTE_PARTITION) {
          serializer
            .writeByte(DataType.BYTE_ARRAY);
          for (Partition partition : partitions) {
            int partitionID =
              partition.getPartitionID();
            serializer.writeInt(partitionID);
            A array =
              ((ArrPartition<A>) partition)
                .getArray();
            ByteArray byteArray =
              (ByteArray) array;
            serializer.writeInt(byteArray
              .getSize());
            serializer.write(
              byteArray.getArray(),
              byteArray.getStart(),
              byteArray.getSize());
          }
        } else if (type == PartitionType.INT_PARTITION) {
          serializer
            .writeByte(DataType.INT_ARRAY);
          for (Partition partition : partitions) {
            int partitionID =
              partition.getPartitionID();
            A array =
              ((ArrPartition<A>) partition)
                .getArray();
            serializer.writeInt(partitionID);
            IntArray intArray = (IntArray) array;
            DataUtils.serializeIntArray(intArray,
              serializer);
          }
        } else if (type == PartitionType.DOUBLE_PARTITION) {
          serializer
            .writeByte(DataType.DOUBLE_ARRAY);
          for (Partition partition : partitions) {
            int partitionID =
              partition.getPartitionID();
            serializer.writeInt(partitionID);
            A array =
              ((ArrPartition<A>) partition)
                .getArray();
            DoubleArray doubleArray =
              (DoubleArray) array;
            DataUtils.serializeDoubleArray(
              doubleArray, serializer);
          }
        } else if (type == PartitionType.TUPLE_PARTITION) {
          serializer
            .writeByte(DataType.BYTE_ARRAY);
          for (Partition partition : partitions) {
            TuplePartition tuplePartition =
              (TuplePartition) partition;
            tuplePartition
              .finalizeByteArrays(pool);
            int partitionID =
              tuplePartition.getPartitionID();
            for (ByteArray byteArray : tuplePartition
              .getByteArrays()) {
              serializer.writeInt(partitionID);
              serializer.writeInt(byteArray
                .getSize());
              serializer.write(
                byteArray.getArray(),
                byteArray.getStart(),
                byteArray.getSize());
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Fail to encode partition list.",
        e);
      pool.releaseBytes(bytes);
      bytes = null;
      serializer = null;
      return null;
    }
    return new ByteArray(bytes, 0, size);
  }

  /**
   * We allow the number of partitions to be 0
   * 
   * @param byteArray
   * @param pool
   * @return
   */
  public static <T, A extends Array<T>>
    List<Partition> decodePartitionList(
      final ByteArray byteArray,
      final ResourcePool pool) {
    LinkedList<Partition> partitions =
      new LinkedList<>();
    Deserializer deserializer =
      new Deserializer(byteArray);
    boolean isFailed = false;
    byte dataType = DataType.UNKNOWN_DATA_TYPE;
    try {
      dataType = deserializer.readByte();
    } catch (Exception e) {
      LOG.error("Fail to decode partition list.",
        e);
      isFailed = true;
    }
    if (!isFailed
      && dataType != DataType.UNKNOWN_DATA_TYPE) {
      if (dataType == DataType.STRUCT_OBJECT) {
        while (deserializer.getPos() < deserializer
          .getLength()) {
          Partition partition =
            (Partition) DataUtils
              .deserializeStructObj(deserializer,
                pool);
          if (partition == null) {
            isFailed = true;
            break;
          } else {
            partitions.add(partition);
          }
        }
        if (isFailed) {
          for (Partition partition : partitions) {
            releasePartition(pool, partition,
              PartitionType.STRUCT_PARTITION);
          }
        }
      } else if (dataType == DataType.BYTE_ARRAY) {
        while (deserializer.getPos() < deserializer
          .getLength()) {
          try {
            int partitionID =
              deserializer.readInt();
            ByteArray byteArr =
              DataUtils.deserializeByteArray(
                deserializer, pool);
            if (byteArr != null) {
              Partition partition =
                (Partition) new ArrPartition<ByteArray>(
                  partitionID, byteArr);
              partitions.add(partition);
            } else {
              throw new Exception();
            }
          } catch (Exception e) {
            isFailed = true;
            break;
          }
        }
        if (isFailed) {
          for (Partition partition : partitions) {
            releasePartition(pool, partition,
              PartitionType.BYTE_PARTITION);
          }
        }
      } else if (dataType == DataType.INT_ARRAY) {
        while (deserializer.getPos() < deserializer
          .getLength()) {
          try {
            int partitionID =
              deserializer.readInt();
            IntArray intArr =
              DataUtils.deserializeIntArray(
                deserializer, pool);
            if (intArr != null) {
              Partition partition =
                (Partition) new ArrPartition<IntArray>(
                  partitionID, intArr);
              partitions.add(partition);
            } else {
              throw new Exception();
            }
          } catch (Exception e) {
            isFailed = true;
            break;
          }
        }
        if (isFailed) {
          for (Partition partition : partitions) {
            releasePartition(pool, partition,
              PartitionType.INT_PARTITION);
          }
        }
      } else if (dataType == DataType.DOUBLE_ARRAY) {
        while (deserializer.getPos() < deserializer
          .getLength()) {
          try {
            int partitionID =
              deserializer.readInt();
            DoubleArray doubleArr =
              DataUtils.deserializeDoubleArray(
                deserializer, pool);
            if (doubleArr != null) {
              Partition partition =
                (Partition) new ArrPartition<DoubleArray>(
                  partitionID, doubleArr);
              partitions.add(partition);
            } else {
              throw new Exception();
            }
          } catch (Exception e) {
            isFailed = true;
            break;
          }
        }
        if (isFailed) {
          for (Partition partition : partitions) {
            releasePartition(pool, partition,
              PartitionType.DOUBLE_PARTITION);
          }
        }
      } else {
        LOG.info("Unkown data type.");
      }
    }
    if (isFailed) {
      LOG.info("Fail to decode partitions.");
      partitions.clear();
      return null;
    } else {
      // LOG.info("Succeed to decode partitions.");
      return partitions;
    }
  }

  public static <P extends Partition> boolean
    gatherPartitionDistribution(
      String contextName, String operationName,
      Table<P> table,
      List<PartitionDistr> recvPDistrs,
      DataMap dataMap, Workers workers,
      ResourcePool resourcePool) {
    int selfID = workers.getSelfID();
    int masterID = workers.getMasterID();
    PartitionDistr pDistr =
      resourcePool
        .getWritableObject(PartitionDistr.class);
    pDistr.setWorkerID(selfID);
    pDistr.setParDistr(PartitionUtils
      .getPartitionDistribution(table,
        resourcePool));
    boolean isSuccess =
      Communication.structObjGather(contextName,
        operationName, pDistr, recvPDistrs,
        masterID, dataMap, workers, resourcePool);
    // Clean the queue
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    if (workers.isMaster()) {
      if (isSuccess) {
        // Gather only receive data sent from
        // other workers
        recvPDistrs.add(pDistr);
        return true;
      } else {
        resourcePool
          .releaseWritableObject(pDistr);
        return false;
      }
    } else {
      resourcePool.releaseWritableObject(pDistr);
      if (isSuccess) {
        return true;
      } else {
        return false;
      }
    }
  }

  public static <P extends Partition> boolean
    allgatherPartitionDistribution(
      String contextName, String operationName,
      PartitionDistr pDistr,
      List<PartitionDistr> recvPDistrs,
      int selfID, int numWorkers,
      DataMap dataMap, Workers workers,
      ResourcePool resourcePool) {
    boolean isSuccess =
      Communication.allgather(contextName,
        operationName, DataType.STRUCT_OBJECT,
        pDistr, recvPDistrs, selfID, numWorkers,
        dataMap, workers, resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSuccess;
  }

  public static <P extends Partition> boolean
    allgatherPartitionSet(String contextName,
      String operationName, PartitionSet pSet,
      List<PartitionSet> recvPSet, int selfID,
      int numWorkers, DataMap dataMap,
      Workers workers, ResourcePool resourcePool) {
    boolean isSuccess =
      Communication.allgather(contextName,
        operationName, DataType.STRUCT_OBJECT,
        pSet, recvPSet, selfID, numWorkers,
        dataMap, workers, resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    return isSuccess;
  }

  public static int regroupPartitionDistribution(
    String contextName, String operationName,
    Partition[] partitionList,
    TableType tableType,
    LinkedList<PartitionDistr> recvPDistrs,
    Int2IntOpenHashMap globalPartitionMap,
    boolean useModulo, int selfID,
    int numWorkers, DataMap dataMap,
    Workers workers, ResourcePool resourcePool) {
    Int2IntOpenHashMap parIDCountMap =
      PartitionUtils.getPartitionDistribution(
        tableType, partitionList, resourcePool);
    // Initialize entries in workerParIDCounts
    // for every worker
    Int2IntOpenHashMap[] workerParIDCounts =
      new Int2IntOpenHashMap[numWorkers];
    for (int i = 0; i < numWorkers; i++) {
      Int2IntOpenHashMap parIDCount =
        new Int2IntOpenHashMap();
      parIDCount.defaultReturnValue(0);
      workerParIDCounts[i] = parIDCount;
    }
    // Regroup partition count based on the
    // worker ID
    int numPartitionsInMatch = 0;
    for (Partition partition : partitionList) {
      int partitionID =
        partition.getPartitionID();
      int workerID =
        globalPartitionMap.get(partitionID);
      if (workerID == -1 && useModulo) {
        // Complete the global partition map
        workerID = partitionID % numWorkers;
        globalPartitionMap.put(partitionID,
          workerID);
      }
      if (workerID != -1) {
        // Put the partition ID and its count to
        // the related worker entry
        workerParIDCounts[workerID].put(
          partitionID,
          parIDCountMap.get(partitionID));
        numPartitionsInMatch++;
      }
    }
    // Send and receive partition distribution
    int[] sendOrder = createSendOrder(numWorkers);
    for (int i = 0; i < sendOrder.length; i++) {
      // Create PartitionDistr
      Int2IntOpenHashMap parIDCount =
        workerParIDCounts[sendOrder[i]];
      PartitionDistr pDistr =
        resourcePool
          .getWritableObject(PartitionDistr.class);
      pDistr.setWorkerID(selfID);
      pDistr.setParDistr(parIDCount);
      if (sendOrder[i] != selfID) {
        // Send
        Communication.send(contextName, selfID,
          operationName,
          Constants.UNKNOWN_PARTITION_ID, pDistr,
          true, false, sendOrder[i], workers,
          resourcePool, true);
        resourcePool
          .releaseWritableObject(pDistr);
      } else {
        recvPDistrs.add(pDistr);
      }
    }
    workerParIDCounts = null;
    // Receive local table's partition
    // distribution
    boolean isFailed = false;
    for (int i = 1; i < numWorkers; i++) {
      Data data =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      if (data != null) {
        recvPDistrs.add((PartitionDistr) data
          .getBody());
      } else {
        for (PartitionDistr pDistr : recvPDistrs) {
          resourcePool
            .releaseWritableObject(pDistr);
        }
        recvPDistrs.clear();
        isFailed = true;
        break;
      }
    }
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    if (isFailed) {
      numPartitionsInMatch = -1;
    }
    return numPartitionsInMatch;
  }

  public static int regroupPartitionSet(
    String contextName, String operationName,
    Partition[] partitionList,
    TableType tableType,
    LinkedList<PartitionSet> recvPSets,
    Int2IntOpenHashMap globalPartitionMap,
    boolean useModulo, int selfID,
    int numWorkers, DataMap dataMap,
    Workers workers, ResourcePool resourcePool) {
    IntArrayList parIDs =
      PartitionUtils
        .getPartitionSet(partitionList);
    // Initialize entries in workerParIDCounts
    // for every worker
    IntArrayList[] workerParIDs =
      new IntArrayList[numWorkers];
    for (int i = 0; i < numWorkers; i++) {
      workerParIDs[i] =
        new IntArrayList(partitionList.length);
    }
    // Regroup partition count based on the
    // worker ID
    int numPartitionsInMatch = 0;
    for (Partition partition : partitionList) {
      int partitionID =
        partition.getPartitionID();
      int workerID =
        globalPartitionMap.get(partitionID);
      if (workerID == -1 && useModulo) {
        // Complete the global partition map
        workerID = partitionID % numWorkers;
        globalPartitionMap.put(partitionID,
          workerID);
      }
      if (workerID != -1) {
        // Put the partition ID and its count to
        // the related worker entry
        workerParIDs[workerID].add(partitionID);
        numPartitionsInMatch++;
      }
    }
    // Send and receive partition distribution
    int[] sendOrder = createSendOrder(numWorkers);
    for (int i = 0; i < sendOrder.length; i++) {
      IntArrayList workerParIDSet =
        workerParIDs[sendOrder[i]];
      PartitionSet pSet =
        resourcePool
          .getWritableObject(PartitionSet.class);
      pSet.setWorkerID(selfID);
      pSet.setParSet(workerParIDSet);
      if (sendOrder[i] != selfID) {
        // Send
        Communication.send(contextName, selfID,
          operationName,
          Constants.UNKNOWN_PARTITION_ID,
          DataType.STRUCT_OBJECT, pSet, true,
          false, sendOrder[i], workers,
          resourcePool, true);
        resourcePool.releaseWritableObject(pSet);
      } else {
        recvPSets.add(pSet);
      }
    }
    workerParIDs = null;
    // Receive local table's partition
    // distribution
    boolean isFailed = false;
    for (int i = 1; i < numWorkers; i++) {
      Data data =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      if (data != null) {
        recvPSets.add((PartitionSet) data
          .getBody());
      } else {
        DataUtils.releaseTrans(resourcePool,
          recvPSets, DataType.STRUCT_OBJECT);
        recvPSets.clear();
        isFailed = true;
        break;
      }
    }
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    if (isFailed) {
      numPartitionsInMatch = -1;
    }
    return numPartitionsInMatch;
  }

  public static int regroupPartitionCount(
    String contextName, String operationName,
    Partition[] partitionList,
    TableType tableType,
    LinkedList<PartitionCount> recvPCounts,
    Int2IntOpenHashMap globalPartitionMap,
    boolean useModulo, int selfID,
    int numWorkers, DataMap dataMap,
    Workers workers, ResourcePool resourcePool) {
    // Initialize entries in workerParIDCounts
    // for every worker
    int[] workerParCounts = new int[numWorkers];
    // Regroup partition count based on the
    // worker ID
    int numPartitionsInMatch = 0;
    for (Partition partition : partitionList) {
      int partitionID =
        partition.getPartitionID();
      int workerID =
        globalPartitionMap.get(partitionID);
      if (workerID == -1 && useModulo) {
        // Complete the global partition map
        workerID = partitionID % numWorkers;
        globalPartitionMap.put(partitionID,
          workerID);
      }
      if (workerID != -1) {
        // Put the partition ID and its count to
        // the related worker entry
        workerParCounts[workerID]++;
        numPartitionsInMatch++;
      }
    }
    // Send and receive partition distribution
    int[] sendOrder = createSendOrder(numWorkers);
    for (int i = 0; i < sendOrder.length; i++) {
      int workerParCount =
        workerParCounts[sendOrder[i]];
      PartitionCount pCount =
        resourcePool
          .getWritableObject(PartitionCount.class);
      pCount.setWorkerID(selfID);
      pCount.setPartitionCount(workerParCount);
      if (sendOrder[i] != selfID) {
        // Send
        Communication.send(contextName, selfID,
          operationName,
          Constants.UNKNOWN_PARTITION_ID,
          DataType.STRUCT_OBJECT, pCount, true,
          false, sendOrder[i], workers,
          resourcePool, true);
        resourcePool
          .releaseWritableObject(pCount);
      } else {
        recvPCounts.add(pCount);
      }
    }
    workerParCounts = null;
    // Receive local table's partition
    // distribution
    boolean isFailed = false;
    for (int i = 1; i < numWorkers; i++) {
      Data data =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      if (data != null) {
        recvPCounts.add((PartitionCount) data
          .getBody());
      } else {
        DataUtils.releaseTrans(resourcePool,
          recvPCounts, DataType.STRUCT_OBJECT);
        recvPCounts.clear();
        isFailed = true;
        break;
      }
    }
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    if (isFailed) {
      numPartitionsInMatch = -1;
    }
    return numPartitionsInMatch;
  }

  public static <P extends Partition> boolean
    receivePartitions(String contextName,
      String operationName, Table<P> table,
      int numRecvPartitions,
      List<Integer> rmPartitionIDs,
      DataMap dataMap, ResourcePool resourcePool) {
    LinkedList<Partition> recvPartitions =
      new LinkedList<>();
    // Start receiving
    Data data = null;
    boolean isSuccess = true;
    for (int i = 0; i < numRecvPartitions; i++) {
      data =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      if (data != null) {
        addPartitionFromData(recvPartitions, data);
        data = null;
      } else {
        isSuccess = false;
        break;
      }
    }
    if (isSuccess) {
      for (int partitionID : rmPartitionIDs) {
        P partition =
          table.removePartition(partitionID);
        releasePartition(resourcePool, partition);
      }
      // Add the received partition to the table
      // Release combined partitions
      for (Partition partition : recvPartitions) {
        addPartitionToTable(partition, table,
          resourcePool);
      }
      return true;
    } else {
      while (!recvPartitions.isEmpty()) {
        Partition partition =
          recvPartitions.removeFirst();
        releasePartition(resourcePool, partition);
      }
      return false;
    }
  }

  public static <P extends Partition> boolean
    receivePartitionsAlt(String contextName,
      String operationName, Table<P> table,
      int numRecvPartitions,
      List<Integer> rmPartitionIDs,
      DataMap dataMap, ResourcePool resourcePool) {
    for (int partitionID : rmPartitionIDs) {
      P partition =
        table.removePartition(partitionID);
      releasePartition(resourcePool, partition);
    }
    LinkedList<Partition> recvPartitions =
      new LinkedList<>();
    // Start receiving
    Data data = null;
    boolean isSuccess = true;
    for (int i = 0; i < numRecvPartitions; i++) {
      data =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      if (data != null) {
        addPartitionFromData(recvPartitions, data);
        data = null;
        addPartitionToTable(
          recvPartitions.removeFirst(), table,
          resourcePool);
      } else {
        isSuccess = false;
        break;
      }
    }
    if (isSuccess) {
      return true;
    } else {
      // Because some partitions have been added
      // to the table, we cannot alter the table
      return false;
    }
  }

  public static <T, A extends Array<T>>
    PartitionType getPartitionType(
      Partition partition) {
    if (partition instanceof StructPartition) {
      return PartitionType.STRUCT_PARTITION;
    } else if (partition.getClass().equals(
      ArrPartition.class)) {
      A array =
        ((ArrPartition<A>) partition).getArray();
      Class<?> arrayClass = array.getClass();
      if (arrayClass.equals(ByteArray.class)) {
        return PartitionType.BYTE_PARTITION;
      } else if (arrayClass
        .equals(IntArray.class)) {
        return PartitionType.INT_PARTITION;
      } else if (arrayClass
        .equals(DoubleArray.class)) {
        return PartitionType.DOUBLE_PARTITION;
      }
    } else if (partition.getClass().equals(
      TuplePartition.class)) {
      return PartitionType.TUPLE_PARTITION;
    }
    return PartitionType.UNKNOWN_PARTITION;
  }

  public static <T, A extends Array<T>> int
    getPartitionSizeInBytes(
      PartitionType partitionType,
      Partition partition, ResourcePool pool) {
    if (partitionType == PartitionType.STRUCT_PARTITION) {
      // struct partition size
      return DataUtils
        .getStructObjSizeInBytes((StructPartition) partition);
    } else if (partitionType == PartitionType.BYTE_PARTITION) {
      // partition ID, array length,
      // array elements
      A array =
        ((ArrPartition<A>) partition).getArray();
      return 8 + array.getSize();
    } else if (partitionType == PartitionType.INT_PARTITION) {
      A array =
        ((ArrPartition<A>) partition).getArray();
      return 8 + array.getSize() * 4;
    } else if (partitionType == PartitionType.DOUBLE_PARTITION) {
      A array =
        ((ArrPartition<A>) partition).getArray();
      return 8 + array.getSize() * 8;
    } else if (partitionType == PartitionType.TUPLE_PARTITION) {
      TuplePartition tuplePartition =
        (TuplePartition) partition;
      tuplePartition.finalizeByteArrays(pool);
      int size = 0;
      for (ByteArray array : tuplePartition
        .getByteArrays()) {
        // partition ID, array
        // length, array elements
        size += (8 + array.getSize());
      }
      return size;
    }
    return 0;
  }

  public static
    <T, A extends Array<T>, P extends Partition>
    TableType getTableType(Table<P> table) {
    if (table.getClass().equals(ArrTable.class)) {
      return TableType.ARRAY_TABLE;
    } else if (table instanceof TupleTable) {
      return TableType.TUPLE_TABLE;
    } else if (table instanceof StructTable) {
      return TableType.STRUCT_TABLE;
    }
    return TableType.UNKNOWN_TABLE;
  }

  public static <P extends Partition> Partition[]
    getPartitionArray(Table<P> table) {
    Partition[] array =
      new Partition[table.getNumPartitions()];
    ObjectIterator<Int2ObjectMap.Entry<P>> iterator =
      table.getPartitionMap()
        .int2ObjectEntrySet().fastIterator();
    int i = 0;
    while (iterator.hasNext()) {
      Int2ObjectMap.Entry<P> entry =
        iterator.next();
      array[i] = entry.getValue();
      i++;
    }
    return array;
  }

  public static int[] createSendOrder(
    int numWorkers) {
    Random random =
      new Random(System.currentTimeMillis());
    int[] sendOrder = new int[numWorkers];
    for (int i = 0; i < numWorkers; i++) {
      sendOrder[i] = i;
    }
    for (int i = numWorkers - 1; i >= 0; i--) {
      int index = random.nextInt(i + 1);
      int tmp = sendOrder[i];
      sendOrder[i] = sendOrder[index];
      sendOrder[index] = tmp;
    }
    return sendOrder;
  }
}
