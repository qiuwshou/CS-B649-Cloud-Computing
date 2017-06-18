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

package edu.iu.harp.collective;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.iu.harp.client.DataChainBcastSender;
import edu.iu.harp.client.DataMSTBcastSender;
import edu.iu.harp.client.DataSender;
import edu.iu.harp.comm.Communication;
import edu.iu.harp.compute.Computation;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.DataStatus;
import edu.iu.harp.io.DataType;
import edu.iu.harp.io.IOUtils;
import edu.iu.harp.message.PartitionCount;
import edu.iu.harp.message.PartitionSet;
import edu.iu.harp.message.SyncReduce;
import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.PartitionCombineTask;
import edu.iu.harp.partition.PartitionCopyTask;
import edu.iu.harp.partition.PartitionDecodeTask;
import edu.iu.harp.partition.PartitionEncodeTask;
import edu.iu.harp.partition.PartitionType;
import edu.iu.harp.partition.PartitionUtils;
import edu.iu.harp.partition.Table;
import edu.iu.harp.partition.TableType;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.worker.Workers;

public class LocalGlobalSyncCollective {

  private static final Logger LOG = Logger
    .getLogger(LocalGlobalSyncCollective.class);

  public static <P extends Partition> boolean
    syncLocalWithGlobal(final String contextName,
      final String operationName,
      Table<P> localTable, Table<P> globalTable,
      boolean useBcast, int numTasks,
      DataMap dataMap, Workers workers,
      ResourcePool resourcePool) {
    return bcastGlobalToLocal(contextName,
      operationName, localTable, globalTable,
      useBcast, numTasks, dataMap, workers,
      resourcePool);
  }

  public static <P extends Partition> boolean
    syncGlobalWithLocal(final String contextName,
      final String operationName,
      Table<P> localTable, Table<P> globalTable,
      boolean useReduce, int numTasks,
      DataMap dataMap, Workers workers,
      ResourcePool resourcePool) {
    return reduceLocalToGlobal(contextName,
      operationName, localTable, globalTable,
      useReduce, numTasks, dataMap, workers,
      resourcePool);
  }

  private static boolean allgather(
    String contextName, String operationName,
    LinkedList<Partition> ownedPartitions,
    LinkedList<Partition> recvPartitions,
    int numAllgatherPartitions, int numTasks,
    Workers workers, DataMap dataMap,
    ResourcePool resourcePool) {
    // Get worker info
    int selfID = workers.getSelfID();
    int nextID = workers.getNextID();
    // Send out owned partitions
    send(contextName, operationName,
      ownedPartitions, selfID, nextID,
      Constants.SEND, numTasks, workers,
      resourcePool);
    // LOG.info("Partitions are sent");
    // New computation and tasks
    List<PartitionDecodeTask> tasks =
      new LinkedList<>();
    for (int i = 0; i < numTasks; i++) {
      tasks.add(new PartitionDecodeTask(
        resourcePool));
    }
    Computation<PartitionDecodeTask> compute =
      new Computation<>(tasks);
    compute.start();
    // Receive starts
    int recvID = Constants.UNKNOWN_WORKER_ID;
    boolean isFailed = false;
    Data data = null;
    DataSender sender = null;
    for (int i = 0; i < numAllgatherPartitions;) {
      // Wait data
      // long startTime =
      // System.currentTimeMillis();
      data =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      // long endTime =
      // System.currentTimeMillis();
      if (data == null) {
        isFailed = true;
        break;
      }
      recvID = data.getWorkerID();
      // LOG.info("Wait data from Worker " +
      // recvID);
      // Continue sending to your next neighbor
      // until you receive the data you sent
      if (recvID != selfID) {
        // LOG.info("Send data from " + recvID
        // + " to " + nextID);
        sender =
          new DataSender(data, nextID, workers,
            resourcePool, Constants.SEND);
        sender.execute();
      }
      // Partition ID is the number of partitions
      // in the data
      i += data.getPartitionID();
      // The body is not decoded yet
      // Submit for decoding
      compute.submit(data);
    }
    compute.stop();
    Object output = null;
    if (isFailed) {
      do {
        output = compute.waitOutput();
        if (output instanceof Data) {
          ((Data) output).release(resourcePool);
        }
      } while (output != null);
    } else {
      do {
        // LOG.info("Wait on the output");
        output = compute.waitOutput();
        Data recvData = null;
        if (output instanceof Data) {
          recvData = (Data) output;
          // LOG.info("Get the output "
          // + recvData.getHeadStatus() + " "
          // + recvData.getBodyStatus());
          List<Partition> partitions =
            (List<Partition>) recvData.getBody();
          recvPartitions.addAll(partitions);
          recvData.releaseHeadArray(resourcePool);
          recvData.releaseBodyArray(resourcePool);
        }
      } while (output != null);
    }
    return !isFailed;
  }

  private static
    <P extends Partition>
    boolean
    reduceScatter(
      final String contextName,
      final String operationName,
      final LinkedList<Partition> ownedPartitions,
      final LinkedList<Partition> recvPartitions,
      final Table<P> sourceTable,
      final TableType tableType,
      final Int2IntOpenHashMap partitionTargetWorkerMap,
      int numTasks, final Workers workers,
      final int numWorkers,
      final DataMap dataMap,
      final ResourcePool resourcePool) {
    // long t1 = System.currentTimeMillis();
    // Remove partitions in source table
    // use source table to do reduce-scatter
    // recover them back later
    Partition[] sourcePartitions =
      PartitionUtils
        .getPartitionArray(sourceTable);
    PartitionType partitionType =
      PartitionUtils
        .getPartitionType(sourcePartitions[0]);
    sourceTable.getPartitionMap().clear();
    // Put rs partitions to the source table
    for (Partition partition : ownedPartitions) {
      PartitionUtils.addPartitionToTable(
        partition, partitionType, sourceTable,
        tableType, resourcePool);
    }
    // Regroup rs partitions IDs based on the
    // target workers
    IntArrayList[] targetWorkerParIDMap =
      new IntArrayList[numWorkers];
    ObjectIterator<Int2IntMap.Entry> iterator =
      partitionTargetWorkerMap.int2IntEntrySet()
        .fastIterator();
    while (iterator.hasNext()) {
      Int2IntMap.Entry entry = iterator.next();
      int partitionID = entry.getIntKey();
      int targetWorkerID = entry.getIntValue();
      if (targetWorkerParIDMap[targetWorkerID] == null) {
        targetWorkerParIDMap[targetWorkerID] =
          new IntArrayList();
      }
      targetWorkerParIDMap[targetWorkerID]
        .add(partitionID);
    }
    // long t2 = System.currentTimeMillis();
    // Start reduce-scatter
    final int self = workers.getSelfID();
    int left = workers.getMinID();
    int right = workers.getMaxID();
    int middle = workers.getMiddleID();
    int half = middle - left + 1;
    int range = right - left + 1;
    boolean firstRound = true;
    boolean isFailed = false;
    // Dest: send dest in each sending
    // Target: final destination
    int dest = -1;
    IntArrayList sendTargets = new IntArrayList();
    // source worker ID -> target worker IDs
    int source = -1;
    IntArrayList recvTargets = new IntArrayList();
    int extraSource = -1;
    IntArrayList extraRecvTargets =
      new IntArrayList();
    LinkedList<Partition> sendRSPartitions =
      new LinkedList<>();
    LinkedList<Partition> recvRSPartitions =
      new LinkedList<>();
    Int2ObjectOpenHashMap<LinkedList<Data>> cachedDataMap =
      new Int2ObjectOpenHashMap<>();
    roundloop:
    while (left < right) {
      // Go through each target, find the dest
      // worker in this round of sending and
      // receiving
      // long t21 = System.currentTimeMillis();
      for (int target = 0; target < targetWorkerParIDMap.length; target++) {
        if (targetWorkerParIDMap[target] == null) {
          continue;
        }
        if (self <= middle && target > middle) {
          dest = self + half;
          // If the range is odd, middle's destID
          // will be out of range.
          if (dest > right) {
            dest = middle + 1;
          }
          // Record dest in this sending
          // and all the final targets
          sendTargets.add(target);
        } else if (self > middle
          && target <= middle) {
          int destID = self - half;
          sendTargets.add(target);
        } else if (self <= middle
          && target <= middle) {
          source = self + half;
          // Record sending source
          // and the final targets
          if (source <= right) {
            recvTargets.add(target);
          }
        } else if (self > middle
          && target > middle) {
          source = self - half;
          recvTargets.add(target);
          if (range % 2 == 1
            && self == (middle + 1)) {
            extraSource = middle;
            recvTargets.add(target);
          }
        }
      }
      // LOG.info("left " + left + ", right "
      // + right + ", middle " + middle
      // + ", half " + half + ", range " + range
      // + ", selfID " + selfID);
      // Send the partitions
      for (int targetID : sendTargets) {
        for (int partitionID : targetWorkerParIDMap[targetID]) {
          // Remove the partitions in the
          // source table
          sendRSPartitions.add(sourceTable
            .removePartition(partitionID));
        }
      }
      send(contextName, operationName,
        sendRSPartitions, self, dest,
        Constants.SEND_DECODE, numTasks, workers,
        resourcePool);
      // Release partitions sent out
      if (!firstRound) {
        PartitionUtils.releasePartitions(
          resourcePool, sendRSPartitions,
          partitionType);
      }
      sendRSPartitions.clear();
      if (!recvTargets.isEmpty()) {
        if (firstRound) {
          // Copy existing partitions in the
          // source table
          Partition[] rsPartitions =
            PartitionUtils
              .getPartitionArray(sourceTable);
          Partition[] copiedRSPartitions =
            new Partition[rsPartitions.length];
          boolean isSuccess =
            PartitionUtils
              .copyPartitionsInMultiTasks(
                rsPartitions, copiedRSPartitions,
                partitionType, numTasks,
                resourcePool);
          if (!isSuccess) {
            isFailed = true;
            break roundloop;
          }
          sourceTable.getPartitionMap().clear();
          for (Partition partition : copiedRSPartitions) {
            PartitionUtils.addPartitionToTable(
              partition, partitionType,
              sourceTable, tableType,
              resourcePool);
          }
          firstRound = false;
        }
        int recvCount = 0;
        int curRecvCount = 0;
        // Count the num of partitions to receive
        // and check how many of them have been
        // received
        {
          for (int targetID : recvTargets) {
            recvCount +=
              targetWorkerParIDMap[targetID]
                .size();
          }
          LinkedList<Data> cachedDataList =
            cachedDataMap.get(source);
          if (cachedDataList != null) {
            for (Data data : cachedDataList) {
              curRecvCount +=
                data.getPartitionID();
              // Add partitions
              recvRSPartitions
                .addAll((List<Partition>) data
                  .getBody());
            }
            cachedDataList.clear();
          }
        }
        if (!extraRecvTargets.isEmpty()) {
          for (int targetID : extraRecvTargets) {
            recvCount +=
              targetWorkerParIDMap[targetID]
                .size();
          }
          LinkedList<Data> cachedDataList =
            cachedDataMap.get(extraSource);
          if (cachedDataList != null) {
            for (Data data : cachedDataList) {
              curRecvCount +=
                data.getPartitionID();
              // Add partitions
              recvRSPartitions
                .addAll((List<Partition>) data
                  .getBody());
            }
            cachedDataList.clear();
          }
        }
        // Continue waiting for more data
        while (curRecvCount < recvCount) {
          Data data =
            IOUtils.waitAndGet(dataMap,
              contextName, operationName);
          if (data == null) {
            isFailed = true;
            break roundloop;
          } else if (data.getWorkerID() != source
            && data.getWorkerID() != extraSource) {
            // If this data is not considered for
            // receiving in this round
            LinkedList<Data> cachedDataList =
              cachedDataMap.get(data
                .getWorkerID());
            if (cachedDataList == null) {
              cachedDataList = new LinkedList<>();
              cachedDataMap.put(
                data.getWorkerID(),
                cachedDataList);
            }
            cachedDataList.add(data);
          } else {
            curRecvCount += data.getPartitionID();
            // Add partitions
            recvRSPartitions
              .addAll((List<Partition>) data
                .getBody());
          }
        }
        PartitionType recvPartitionType =
          PartitionUtils
            .getPartitionType(recvRSPartitions
              .getFirst());
        combinePartitionsToTable(
          recvRSPartitions, recvPartitionType,
          sourceTable, tableType, numTasks,
          resourcePool);
        recvRSPartitions.clear();
      }
      if (self <= middle) {
        right = middle;
      } else {
        left = middle + 1;
      }
      middle = (left + right) / 2;
      half = middle - left + 1;
      range = right - left + 1;
      sendTargets.clear();
      recvTargets.clear();
      extraRecvTargets.clear();
    }
    if (isFailed) {
      // Clean received partitions
      PartitionType recvPartitionType =
        PartitionUtils
          .getPartitionType(recvRSPartitions
            .getFirst());
      for (Partition recvRSPartition : recvRSPartitions) {
        PartitionUtils.releasePartition(
          resourcePool, recvRSPartition,
          recvPartitionType);
      }
      // Clean cached data map
      for (LinkedList<Data> dataList : cachedDataMap
        .values()) {
        for (Data d : dataList) {
          d.release(resourcePool);
        }
      }
      // Clean source table
      if (!firstRound) {
        // source table contents was copied from
        // original table, release
        PartitionUtils.releaseTable(resourcePool,
          sourceTable, partitionType);
      } else {
        // source table contents was from original
        // table, it was never copied
        sourceTable.getPartitionMap().clear();
      }
      // Restore source partitions to source
      // table
      for (Partition partition : sourcePartitions) {
        PartitionUtils.addPartitionToTable(
          partition, partitionType, sourceTable,
          tableType, resourcePool);
      }
      return false;
    }
    // long t3 = System.currentTimeMillis();
    // Put rs partitions in rs partition map
    // to recv partitions
    recvPartitions.addAll(sourceTable
      .getPartitions());
    sourceTable.getPartitionMap().clear();
    // Restore source partitions to source table
    for (Partition partition : sourcePartitions) {
      PartitionUtils.addPartitionToTable(
        partition, partitionType, sourceTable,
        tableType, resourcePool);
    }
    // long t4 = System.currentTimeMillis();
    // LOG.info("Reduce-scatter: " + (t4 - t3) +
    // " "
    // + (t3 - t2) + " " + (t2 - t1));
    return true;
  }

  private static void send(String contextName,
    String operationName,
    LinkedList<Partition> ownedPartitions,
    int selfID, int destID, byte cmd,
    int numEncodeTasks, Workers workers,
    ResourcePool resourcePool) {
    Computation<PartitionEncodeTask> compute =
      null;
    final int numPartitions =
      ownedPartitions.size();
    LinkedList<Partition> sendPartitions =
      new LinkedList<>();
    PartitionType type =
      PartitionUtils
        .getPartitionType(ownedPartitions
          .getFirst());
    int partitionSize = 1;
    int partitionCount = 0;
    int sendCount = 0;
    for (Partition partition : ownedPartitions) {
      partitionCount++;
      sendPartitions.add(partition);
      partitionSize +=
        PartitionUtils.getPartitionSizeInBytes(
          type, partition, resourcePool);
      if (partitionSize > Constants.MAX_PARTITION_SEND_BYTE_SIZE
        || partitionCount == numPartitions) {
        sendCount++;
        Data data =
          new Data(DataType.PARTITION_LIST,
            contextName, selfID, operationName,
            sendPartitions.size(), sendPartitions);
        if (sendCount == 1
          && partitionCount == numPartitions) {
          DataSender sender =
            new DataSender(data, destID, workers,
              resourcePool, cmd);
          sender.execute();
          data.releaseHeadArray(resourcePool);
          data.releaseEncodedBody(resourcePool);
        } else {
          if (sendCount == 1) {
            LinkedList<PartitionEncodeTask> tasks =
              new LinkedList<>();
            for (int i = 0; i < numEncodeTasks; i++) {
              tasks.add(new PartitionEncodeTask(
                resourcePool));
            }
            compute = new Computation<>(tasks);
            compute.start();
          }
          compute.submit(data);
          sendPartitions = new LinkedList<>();
          partitionSize = 1;
        }
      }
    }
    // When multiple tasks are used
    if (sendCount > 1) {
      compute.stop();
      Object output = null;
      do {
        output = compute.waitOutput();
        if (output != null
          && output instanceof Data) {
          Data data = (Data) output;
          DataSender sender =
            new DataSender(data, destID, workers,
              resourcePool, cmd);
          sender.execute();
          data.releaseHeadArray(resourcePool);
          data.releaseEncodedBody(resourcePool);
        }
      } while (output != null);
    }
  }

  private static void broadcast(
    String contextName, String operationName,
    LinkedList<Partition> ownedPartitions,
    int selfID, int numEncodeTasks,
    Workers workers, ResourcePool resourcePool) {
    Computation<PartitionEncodeTask> compute =
      null;
    final int numPartitions =
      ownedPartitions.size();
    LinkedList<Partition> bcastPartitions =
      new LinkedList<>();
    PartitionType type =
      PartitionUtils
        .getPartitionType(ownedPartitions
          .getFirst());
    int partitionSize = 1;
    int partitionCount = 0;
    int sendCount = 0;
    for (Partition partition : ownedPartitions) {
      partitionCount++;
      bcastPartitions.add(partition);
      partitionSize +=
        PartitionUtils.getPartitionSizeInBytes(
          type, partition, resourcePool);
      // If more than 100 MB
      if (partitionSize > Constants.MAX_PARTITION_SEND_BYTE_SIZE
        || partitionCount == numPartitions) {
        sendCount++;
        Data data =
          new Data(DataType.PARTITION_LIST,
            contextName, selfID, operationName,
            bcastPartitions.size(),
            bcastPartitions);
        if (sendCount == 1
          && partitionCount == numPartitions) {
          data.encodeHead(resourcePool);
          data.encodeBody(resourcePool);
          if (data.getBodyArray() != null) {
            if (data.getBodyArray().getSize() > Constants.MAX_MST_BCAST_BYTE_SIZE) {
              DataChainBcastSender bcaster =
                new DataChainBcastSender(data,
                  workers, resourcePool,
                  Constants.CHAIN_BCAST_DECODE);
              bcaster.execute();
            } else {
              DataMSTBcastSender bcaster =
                new DataMSTBcastSender(data,
                  workers, resourcePool,
                  Constants.MST_BCAST_DECODE);
              bcaster.execute();
            }
          }
          data.releaseHeadArray(resourcePool);
          data.releaseEncodedBody(resourcePool);
        } else {
          if (sendCount == 1) {
            List<PartitionEncodeTask> tasks =
              new LinkedList<>();
            for (int i = 0; i < numEncodeTasks; i++) {
              tasks.add(new PartitionEncodeTask(
                resourcePool));
            }
            compute = new Computation<>(tasks);
            compute.start();
          }
          compute.submit(data);
          bcastPartitions = new LinkedList<>();
          partitionSize = 1;
        }
      }
    }
    if (sendCount > 1) {
      Object output = null;
      do {
        output = compute.waitOutput();
        if (output != null
          && output instanceof Data) {
          Data data = (Data) output;
          if (data.getBodyArray() != null) {
            if (data.getBodyArray().getSize() > Constants.MAX_MST_BCAST_BYTE_SIZE) {
              DataChainBcastSender bcaster =
                new DataChainBcastSender(data,
                  workers, resourcePool,
                  Constants.CHAIN_BCAST_DECODE);
              bcaster.execute();
            } else {
              DataMSTBcastSender bcaster =
                new DataMSTBcastSender(data,
                  workers, resourcePool,
                  Constants.MST_BCAST_DECODE);
              bcaster.execute();
            }
          }
          data.releaseHeadArray(resourcePool);
          data.releaseEncodedBody(resourcePool);
        }
      } while (output != null);
      compute.stop();
    }
  }

  public static
    void
    dispatch(
      final String contextName,
      final String operationName,
      final LinkedList<Partition>[] ownedPartitions,
      final int numSendWorkers, final int selfID,
      final int numTasks, final Workers workers,
      final ResourcePool resourcePool) {
    List<DispatchTask> tasks = new LinkedList<>();
    for (int i = 0; i < numTasks; i++) {
      tasks.add(new DispatchTask(resourcePool));
    }
    Computation<DispatchTask> compute =
      new Computation<>(tasks);
    compute.start();
    Random random = new Random(System.nanoTime());
    LinkedList<Partition> sendPartitions =
      new LinkedList<>();
    PartitionType type =
      PartitionType.UNKNOWN_PARTITION;
    int[] sendOrder =
      PartitionUtils
        .createSendOrder(ownedPartitions.length);
    for (int i = 0; i < sendOrder.length; i++) {
      final LinkedList<Partition> partitionList =
        ownedPartitions[sendOrder[i]];
      if (partitionList != null) {
        if (type == PartitionType.UNKNOWN_PARTITION) {
          type =
            PartitionUtils
              .getPartitionType(partitionList
                .getFirst());
        }
        final int numPartitions =
          partitionList.size();
        int partitionCount = 0;
        int partitionSize = 1;
        for (Partition partition : partitionList) {
          sendPartitions.add(partition);
          partitionCount++;
          partitionSize +=
            PartitionUtils
              .getPartitionSizeInBytes(type,
                partition, resourcePool);
          if (partitionSize > Constants.MAX_PARTITION_SEND_BYTE_SIZE
            || partitionCount == numPartitions) {
            Data data =
              new Data(DataType.PARTITION_LIST,
                contextName, selfID,
                operationName,
                sendPartitions.size(),
                sendPartitions);
            compute.submit(new DispatchData(data,
              sendOrder[i]));
            sendPartitions = new LinkedList<>();
            partitionSize = 1;
          }
        }
      }
    }
    for (int i = 0; i < numSendWorkers; i++) {
      Object output = compute.waitOutput();
      if (output != null) {
        DispatchData dispatchData =
          (DispatchData) output;
        DataSender sender =
          new DataSender(dispatchData.data,
            dispatchData.destID, workers,
            resourcePool, Constants.SEND_DECODE);
        sender.execute();
        // Release
        dispatchData.data
          .releaseHeadArray(resourcePool);
        dispatchData.data
          .releaseEncodedBody(resourcePool);
      }
    }
    compute.stop();
  }

  private static void copy(String contextName,
    String operationName,
    LinkedList<Partition> ownedPartitions,
    int selfID, int numCopyTasks,
    Workers workers, DataMap dataMap,
    ResourcePool resourcePool) {
    Computation<PartitionCopyTask> compute = null;
    final int numPartitions =
      ownedPartitions.size();
    LinkedList<Partition> localPartitions =
      new LinkedList<>();
    PartitionType type =
      PartitionUtils
        .getPartitionType(ownedPartitions
          .getFirst());
    int partitionSize = 1;
    int partitionCount = 0;
    int copyCount = 0;
    for (Partition partition : ownedPartitions) {
      localPartitions.add(partition);
      partitionCount++;
      partitionSize +=
        PartitionUtils.getPartitionSizeInBytes(
          type, partition, resourcePool);
      // If more than 100 MB
      if (partitionSize > Constants.MAX_PARTITION_SEND_BYTE_SIZE
        || partitionCount == numPartitions) {
        copyCount++;
        final Data data =
          new Data(DataType.PARTITION_LIST,
            contextName, selfID, operationName,
            localPartitions.size(),
            localPartitions);
        if (copyCount == 1
          && partitionCount == numPartitions) {
          data.encodeHead(resourcePool);
          data.encodeBody(resourcePool);
          final Data newData =
            new Data(data.getHeadArray(),
              data.getBodyArray());
          newData.decodeHeadArray(resourcePool);
          newData.releaseHeadArray(resourcePool);
          newData.decodeBodyArray(resourcePool);
          newData.releaseBodyArray(resourcePool);
          if ((newData.getHeadStatus() == DataStatus.DECODED && newData
            .getBodyStatus() == DataStatus.DECODED)
            && newData.isOperationData()) {
            dataMap.putData(newData);
          }
        } else {
          if (copyCount == 1) {
            List<PartitionCopyTask> tasks =
              new LinkedList<>();
            for (int i = 0; i < numCopyTasks; i++) {
              tasks.add(new PartitionCopyTask(
                resourcePool));
            }
            compute = new Computation<>(tasks);
            compute.start();
          }
          compute.submit(data);
          localPartitions = new LinkedList<>();
          partitionSize = 1;
        }
      }
    }
    if (copyCount > 1) {
      Object output = null;
      do {
        output = compute.waitOutput();
        if (output != null
          && output instanceof Data) {
          Data data = (Data) output;
          if ((data.getHeadStatus() == DataStatus.DECODED && data
            .getBodyStatus() == DataStatus.DECODED)
            && data.isOperationData()) {
            dataMap.putData(data);
          }
        }
      } while (output != null);
      compute.stop();
    }
  }

  public static <P extends Partition> void
    addPartitionsToTable(
      LinkedList<Partition> partitions,
      PartitionType partitionType,
      Table<P> table, TableType tableType,
      int numWorkers, int numTasks,
      ResourcePool resourcePool) {
    // Create a task partition map
    LinkedList<Partition>[] taskPartitionMap =
      new LinkedList[numTasks];
    int realNumTasks = 0;
    for (Partition partition : partitions) {
      final int partitionID =
        partition.getPartitionID();
      if (table.getPartitionMap().containsKey(
        partitionID)) {
        final int taskID =
          (partitionID / numWorkers) % numTasks;
        if (taskPartitionMap[taskID] == null) {
          taskPartitionMap[taskID] =
            new LinkedList<>();
          realNumTasks++;
        }
        taskPartitionMap[taskID].add(partition);
      } else {
        PartitionUtils.addPartitionToTable(
          partition, partitionType, table,
          tableType, resourcePool);
      }
    }
    if (realNumTasks > 1) {
      // Create tasks
      LinkedList<PartitionCombineTask> tasks =
        new LinkedList<>();
      for (int i = 0; i < realNumTasks; i++) {
        tasks
          .add(new PartitionCombineTask(table,
            resourcePool, tableType,
            partitionType));
      }
      Computation<PartitionCombineTask> compute =
        new Computation<>(tasks);
      compute.submitAll(taskPartitionMap);
      compute.start();
      Object output = null;
      do {
        output = compute.waitOutput();
      } while (output != null);
      compute.stop();
    } else {
      for (LinkedList<Partition> partitionList : taskPartitionMap) {
        if (partitionList != null) {
          for (Partition partition : partitionList) {
            Partition curPartition =
              table.getPartition(partition
                .getPartitionID());
            PartitionUtils
              .combinePartitionToTable(
                curPartition, partition,
                partitionType, table, tableType,
                resourcePool);
          }
        }
      }
    }
  }

  private static <P extends Partition> void
    combinePartitionsToTable(
      final LinkedList<Partition> partitions,
      final PartitionType partitionType,
      final Table<P> table,
      final TableType tableType,
      final int numTasks,
      final ResourcePool resourcePool) {
    // Create a task partition map
    LinkedList<Partition>[] taskPartitionMap =
      new LinkedList[numTasks];
    for (Partition partition : partitions) {
      final int partitionID =
        partition.getPartitionID();
      final int taskID = partitionID % numTasks;
      if (taskPartitionMap[taskID] == null) {
        taskPartitionMap[taskID] =
          new LinkedList<>();
      }
      taskPartitionMap[taskID].add(partition);
    }
    // Create tasks
    LinkedList<PartitionCombineTask> tasks =
      new LinkedList<>();
    for (int i = 0; i < numTasks; i++) {
      tasks.add(new PartitionCombineTask(table,
        resourcePool, tableType, partitionType));
    }
    Computation<PartitionCombineTask> compute =
      new Computation<>(tasks);
    compute.submitAll(taskPartitionMap);
    compute.start();
    Object output = null;
    do {
      output = compute.waitOutput();
    } while (output != null);
    compute.stop();
  }

  public static <P extends Partition> boolean
    reduceLocalToGlobal(final String contextName,
      final String operationName,
      final Table<P> localTable,
      final Table<P> globalTable,
      final boolean useReduce,
      final int numTasks, final DataMap dataMap,
      final Workers workers,
      final ResourcePool resourcePool) {
    LOG.info("reduceLocalToGlobal starts");
    // long time0 = System.currentTimeMillis();
    final int selfID = workers.getSelfID();
    final int numWorkers =
      workers.getNumWorkers();
    // ---------------------------------------------------
    // Broadcast global table's partition
    // distribution to all the workers
    Partition[] ownedGlobalPartitions =
      PartitionUtils
        .getPartitionArray(globalTable);
    TableType tableType =
      PartitionUtils.getTableType(globalTable);
    PartitionSet pSet =
      resourcePool
        .getWritableObject(PartitionSet.class);
    pSet.setWorkerID(selfID);
    pSet.setParSet(PartitionUtils
      .getPartitionSet(ownedGlobalPartitions));
    LinkedList<PartitionSet> globalPSets =
      new LinkedList<>();
    boolean isSuccess =
      PartitionUtils.allgatherPartitionSet(
        contextName, operationName
          + ".allgather.global", pSet,
        globalPSets, selfID, numWorkers, dataMap,
        workers, resourcePool);
    LOG.info("reduceLocalToGlobal allgather");
    if (!isSuccess) {
      return false;
    }
    // --------------------------------------------
    Int2IntOpenHashMap globalPartitionMap =
      new Int2IntOpenHashMap();
    globalPartitionMap.defaultReturnValue(-1);
    for (PartitionSet recvPSet : globalPSets) {
      final IntArrayList pSetMap =
        recvPSet.getParSet();
      if (pSetMap != null) {
        int workerID = recvPSet.getWorkerID();
        for (int i = 0; i < pSetMap.size(); i++) {
          globalPartitionMap.put(
            pSetMap.getInt(i), workerID);
        }
      }
      resourcePool
        .releaseWritableObject(recvPSet);
    }
    globalPSets = null;
    // ---------------------------------------------
    Partition[] ownedLocalPartitions =
      PartitionUtils
        .getPartitionArray(localTable);
    LinkedList<PartitionSet> localPSets =
      new LinkedList<>();
    int numMatchedPartitions =
      PartitionUtils.regroupPartitionSet(
        contextName, operationName
          + ".regroup.local",
        ownedLocalPartitions, tableType,
        localPSets, globalPartitionMap, true,
        selfID, numWorkers, dataMap, workers,
        resourcePool);
    LOG.info("reduceLocalToGlobal regroup");
    if (numMatchedPartitions == -1) {
      return false;
    }
    // ----------------------------------------------
    // Calculate the partition count to receive
    // And the partitions in reduce
    int numRecvPartitions = 0;
    // Int2IntOpenHashMap reduceParTargetWorkerMap
    // = new Int2IntOpenHashMap();
    // if (useReduce && (numWorkers > 1)) {
    // numRecvPartitions =
    // identifyRSSet(contextName, operationName,
    // localPSets, reduceParTargetWorkerMap,
    // selfID, numWorkers, resourcePool,
    // workers, dataMap);
    // if (numRecvPartitions == -1) {
    // return false;
    // }
    // } else {
    // If not using reduce
    for (PartitionSet recvPSet : localPSets) {
      IntArrayList pSetMap = recvPSet.getParSet();
      if (pSetMap != null) {
        numRecvPartitions += pSetMap.size();
      }
      resourcePool
        .releaseWritableObject(recvPSet);
    }
    localPSets = null;
    // }
    // -----------------------------------------
    // Do reduce-scatter
    // Do send
    // Do receive
    // Build communication lists
    LinkedList<Partition> rsPartitions =
      new LinkedList<>();
    LinkedList<Partition>[] sendPartitionMap =
      new LinkedList[numWorkers];
    int numSendWorkers = 0;
    LinkedList<Partition> localPartitions =
      new LinkedList<>();
    for (Partition partition : ownedLocalPartitions) {
      final int partitionID =
        partition.getPartitionID();
      // if (useReduce
      // && reduceParTargetWorkerMap
      // .containsKey(partitionID)) {
      // rsPartitions.add(partition);
      // } else {
      int workerID =
        globalPartitionMap.get(partitionID);
      if (workerID == selfID) {
        localPartitions.add(partition);
      } else {
        if (sendPartitionMap[workerID] == null) {
          sendPartitionMap[workerID] =
            new LinkedList<>();
          numSendWorkers++;
        }
        sendPartitionMap[workerID].add(partition);
      }
      // }
    }
    // for (int i = 0; i <
    // sendPartitionMap.length; i++) {
    // if (sendPartitionMap[i] != null) {
    // LOG.info("Worker " + i + " "
    // + sendPartitionMap[i].size());
    // }
    // }
    LOG.info("Local partitions: "
      + localPartitions.size());
    // int numSendPartitions = 0;
    // for (LinkedList<Partition> sendList :
    // sendPartitionMap
    // .values()) {
    // numSendPartitions += sendList.size();
    // }
    // LOG.info("Reduce-scatter partitions: "
    // + rsPartitions.size()
    // + ", send partitions: " + numSendPartitions
    // + ", local partitions: "
    // + localPartitions.size());
    // ---------------------------------------------
    // long time1 = System.currentTimeMillis();
    LinkedList<Partition> recvPartitions =
      new LinkedList<>();
    // Do reduce scatter
    // if (useReduce && !rsPartitions.isEmpty()) {
    // isSuccess =
    // reduceScatter(contextName, operationName
    // + ".reducescatter", rsPartitions,
    // recvPartitions, localTable, tableType,
    // reduceParTargetWorkerMap, numTasks,
    // workers, numWorkers, dataMap,
    // resourcePool);
    // dataMap.cleanOperationData(contextName,
    // operationName + ".reducescatter",
    // resourcePool);
    // if (!isSuccess) {
    // for (Partition partition : recvPartitions)
    // {
    // PartitionUtils.releasePartition(
    // resourcePool, partition);
    // }
    // return false;
    // }
    // }
    if (numSendWorkers > 0) {
      dispatch(contextName, operationName,
        sendPartitionMap, numSendWorkers, selfID,
        numTasks, workers, resourcePool);
    }
    // Copy local data first
    if (!localPartitions.isEmpty()) {
      copy(contextName, operationName,
        localPartitions, selfID, numTasks,
        workers, dataMap, resourcePool);
    }
    // ---------------------------------------------
    // Start receiving
    // long time2 = System.currentTimeMillis();
    PartitionType recvPartitionType =
      PartitionType.UNKNOWN_PARTITION;
    for (int i = 0; i < numRecvPartitions;) {
      Data data =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      if (data != null) {
        LinkedList<Partition> partitions =
          (LinkedList<Partition>) data.getBody();
        if (recvPartitionType == PartitionType.UNKNOWN_PARTITION) {
          recvPartitionType =
            PartitionUtils
              .getPartitionType(partitions
                .getFirst());
        }
        recvPartitions.addAll(partitions);
        i += data.getPartitionID();
      } else {
        PartitionUtils.releasePartitions(
          resourcePool, recvPartitions,
          recvPartitionType);
        return false;
      }
    }
    // long time3 = System.currentTimeMillis();
    addPartitionsToTable(recvPartitions,
      recvPartitionType, globalTable, tableType,
      numWorkers, numTasks, resourcePool);
    // long time4 = System.currentTimeMillis();
    // LOG.info("Reduce to global "
    // + (time4 - time0) + ". preparation took: "
    // + (time1 - time0) + ", send took: "
    // + (time2 - time1) + ", recv took: "
    // + (time3 - time2) + ", combine took: "
    // + (time4 - time3));
    return true;
  }

  private static int identifyRSSet(
    String contextName, String operationName,
    LinkedList<PartitionSet> localPSets,
    Int2IntOpenHashMap reduceParTargetWorkerMap,
    int selfID, int numWorkers,
    ResourcePool resourcePool, Workers workers,
    DataMap dataMap) {
    int numRecvPartitions = 0;
    // Identify the partitions for reduce
    // we can enable or disable this
    Int2IntOpenHashMap parSendWorkerCountMap =
      new Int2IntOpenHashMap();
    parSendWorkerCountMap.defaultReturnValue(0);
    for (PartitionSet recvPSet : localPSets) {
      IntArrayList pSetMap = recvPSet.getParSet();
      if (pSetMap != null) {
        for (int partitionID : pSetMap) {
          parSendWorkerCountMap.addTo(
            partitionID, 1);
        }
      }
      resourcePool
        .releaseWritableObject(recvPSet);
    }
    localPSets = null;
    // Find the reduce list based on the source
    // count
    IntArrayList reduceList = new IntArrayList();
    ObjectIterator<Int2IntMap.Entry> iterator =
      parSendWorkerCountMap.int2IntEntrySet()
        .fastIterator();
    while (iterator.hasNext()) {
      Int2IntMap.Entry entry = iterator.next();
      int partiitonID = entry.getIntKey();
      int numSendWorkers = entry.getIntValue();
      if (numSendWorkers == numWorkers) {
        reduceList.add(partiitonID);
      } else {
        numRecvPartitions += numSendWorkers;
      }
    }
    // Allgather the reduce list
    SyncReduce sync =
      resourcePool
        .getWritableObject(SyncReduce.class);
    sync.setWorkerID(selfID);
    sync.setReduceList(reduceList);
    LinkedList<SyncReduce> recvSyncList =
      new LinkedList<>();
    boolean isSuccess =
      Communication.allgather(contextName,
        operationName + ".allgather.sync",
        DataType.STRUCT_OBJECT, sync,
        recvSyncList, selfID, numWorkers,
        dataMap, workers, resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName + ".allgather.sync",
      resourcePool);
    if (!isSuccess) {
      resourcePool.releaseWritableObject(sync);
      return -1;
    } else {
      // Record reduce partition and target
      // worker
      for (SyncReduce sr : recvSyncList) {
        IntArrayList srReduceList =
          sr.getReduceList();
        if (srReduceList != null) {
          int workerID = sr.getWorkerID();
          for (int reduceParID : srReduceList) {
            reduceParTargetWorkerMap.put(
              reduceParID, workerID);
          }
        }
        resourcePool.releaseWritableObject(sr);
      }
      recvSyncList = null;
    }
    return numRecvPartitions;
  }

  public static <P extends Partition> boolean
    bcastGlobalToLocal(final String contextName,
      final String operationName,
      final Table<P> localTable,
      final Table<P> globalTable,
      final boolean useBcast, final int numTasks,
      final DataMap dataMap,
      final Workers workers,
      final ResourcePool resourcePool) {
    final int selfID = workers.getSelfID();
    final int numWorkers =
      workers.getNumWorkers();
    // ---------------------------------------------------
    // Allgather global table's partition
    // distribution to all the workers
    // local table send partition info to the
    // related global worker
    // global table start to broadcast or send
    long time0 = System.currentTimeMillis();
    Partition[] ownedGlobalPartitions =
      PartitionUtils
        .getPartitionArray(globalTable);
    LOG.info("Num of global partitions "
      + ownedGlobalPartitions.length);
    TableType tableType =
      PartitionUtils.getTableType(globalTable);
    PartitionSet pSet =
      resourcePool
        .getWritableObject(PartitionSet.class);
    pSet.setWorkerID(selfID);
    pSet.setParSet(PartitionUtils
      .getPartitionSet(ownedGlobalPartitions));
    LinkedList<PartitionSet> globalPSets =
      new LinkedList<>();
    long time01 = System.currentTimeMillis();
    boolean isSuccess =
      PartitionUtils.allgatherPartitionSet(
        contextName, operationName
          + ".allgather.global", pSet,
        globalPSets, selfID, numWorkers, dataMap,
        workers, resourcePool);
    if (!isSuccess) {
      resourcePool.releaseWritableObject(pSet);
      return false;
    }
    long time02 = System.currentTimeMillis();
    // -----------------------------------------------
    // Record partition distribution on the global
    // table and what is contained at the self
    // worker
    Int2IntOpenHashMap globalParDistrMap =
      new Int2IntOpenHashMap();
    globalParDistrMap.defaultReturnValue(-1);
    for (PartitionSet recvPSet : globalPSets) {
      IntArrayList pIDList = recvPSet.getParSet();
      if (pIDList != null) {
        int workerID = recvPSet.getWorkerID();
        for (int i = 0; i < pIDList.size(); i++) {
          // partition ID -> worker ID
          globalParDistrMap.put(
            pIDList.getInt(i), workerID);
        }
      }
      resourcePool
        .releaseWritableObject(recvPSet);
    }
    globalPSets = null;
    long time03 = System.currentTimeMillis();
    // -----------------------------------------------
    // Regroup local table info to global table
    // locations, let the global table holders
    // know the data sending request
    Partition[] ownedLocalPartitions =
      PartitionUtils
        .getPartitionArray(localTable);
    LinkedList<PartitionSet> localPSets =
      new LinkedList<>();
    long time04 = System.currentTimeMillis();
    int numRecvPartitions =
      PartitionUtils.regroupPartitionSet(
        contextName, operationName
          + ".regroup.local",
        ownedLocalPartitions, tableType,
        localPSets, globalParDistrMap, false,
        selfID, numWorkers, dataMap, workers,
        resourcePool);
    if (numRecvPartitions == -1) {
      return false;
    }
    long time05 = System.currentTimeMillis();
    // ----------------------------------------------
    // Detect which and where to send
    Int2ObjectOpenHashMap<IntArrayList> globalParRecvWorkerMap =
      new Int2ObjectOpenHashMap<>();
    for (PartitionSet recvPSet : localPSets) {
      IntArrayList pSetMap = recvPSet.getParSet();
      if (pSetMap != null) {
        int workerID = recvPSet.getWorkerID();
        for (int i = 0; i < pSetMap.size(); i++) {
          int partitionID = pSetMap.getInt(i);
          IntArrayList recvWorkerIDs =
            globalParRecvWorkerMap
              .get(partitionID);
          if (recvWorkerIDs == null) {
            recvWorkerIDs = new IntArrayList();
            globalParRecvWorkerMap.put(
              partitionID, recvWorkerIDs);
          }
          recvWorkerIDs.add(workerID);
        }
      }
      resourcePool
        .releaseWritableObject(recvPSet);
    }
    localPSets = null;
    long time06 = System.currentTimeMillis();
    // -----------------------------------------
    // Build communication lists
    LinkedList<Partition> bcastPartitions =
      new LinkedList<>();
    LinkedList<Partition> localPartitions =
      new LinkedList<>();
    LinkedList<Partition>[] sendPartitions =
      new LinkedList[numWorkers];
    int numSendWorkers = 0;
    for (Partition partition : ownedGlobalPartitions) {
      int partitionID =
        partition.getPartitionID();
      IntArrayList recvWorkerIDs =
        globalParRecvWorkerMap.get(partitionID);
      if (recvWorkerIDs != null) {
        if (useBcast
          && recvWorkerIDs.size() == numWorkers) {
          if (numWorkers > 1) {
            bcastPartitions.add(partition);
          }
          localPartitions.add(partition);
        } else {
          for (int i = 0; i < recvWorkerIDs
            .size(); i++) {
            int recvWorkerID =
              recvWorkerIDs.getInt(i);
            if (recvWorkerID == selfID) {
              localPartitions.add(partition);
            } else {
              if (sendPartitions[recvWorkerID] == null) {
                sendPartitions[recvWorkerID] =
                  new LinkedList<>();
                numSendWorkers++;
              }
              sendPartitions[recvWorkerID]
                .add(partition);
            }
          }
        }
      }
    }
    long time07 = System.currentTimeMillis();
    LOG.info(globalParRecvWorkerMap.size()
      + " Time0 " + (time07 - time06) + " "
      + (time06 - time05) + " "
      + (time04 - time03) + " "
      + (time03 - time02) + " "
      + (time02 - time01) + " "
      + (time01 - time0));
    // -----------------------------------------
    final int numBcastPartitions =
      bcastPartitions.size();
    long time1 = System.currentTimeMillis();
    // Then send/broadcast partitions
    if (!bcastPartitions.isEmpty()) {
      broadcast(contextName, operationName,
        bcastPartitions, selfID, numTasks,
        workers, resourcePool);
    }
    if (numSendWorkers > 0) {
      dispatch(contextName, operationName,
        sendPartitions, numWorkers, selfID,
        numTasks, workers, resourcePool);
    }
    // Copy local data first
    if (!localPartitions.isEmpty()) {
      copy(contextName, operationName,
        localPartitions, selfID, numTasks,
        workers, dataMap, resourcePool);
    }
    // ---------------------------------------------
    long time2 = System.currentTimeMillis();
    // Receive all the partitions
    // Start receiving
    LinkedList<Partition> recvPartitions =
      new LinkedList<>();
    PartitionType recvPartitionType =
      PartitionType.UNKNOWN_PARTITION;
    for (int i = 0; i < numRecvPartitions;) {
      Data data =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      if (data != null) {
        LinkedList<Partition> partitions =
          (LinkedList<Partition>) data.getBody();
        if (recvPartitionType == PartitionType.UNKNOWN_PARTITION) {
          recvPartitionType =
            PartitionUtils
              .getPartitionType(partitions
                .getFirst());
        }
        recvPartitions.addAll(partitions);
        i += data.getPartitionID();
      } else {
        PartitionUtils.releasePartitions(
          resourcePool, recvPartitions,
          recvPartitionType);
        return false;
      }
    }
    long time3 = System.currentTimeMillis();
    combinePartitionsToTable(recvPartitions,
      recvPartitionType, localTable, tableType,
      numTasks, resourcePool);
    long time4 = System.currentTimeMillis();
    LOG.info("Broadcast to local "
      + (time4 - time0)
      + ".\n Preparation took: "
      + (time1 - time0) + ",\n send took: "
      + (time2 - time1) + ",\n recv took: "
      + (time3 - time2) + ",\n combine took: "
      + (time4 - time3)
      + ", num of bcast partitions: "
      + numBcastPartitions);
    return true;
  }

  public static <P extends Partition> boolean
    rotateGlobal(final String contextName,
      final String operationName,
      Table<P> globalTable, DataMap dataMap,
      Workers workers, ResourcePool resourcePool) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    final int selfID = workers.getSelfID();
    final int nextID = workers.getNextID();
    // -------------------------------------------
    return rotate(contextName, operationName,
      globalTable, selfID, nextID, dataMap,
      workers, resourcePool);
  }

  public static <P extends Partition> boolean
    rotateGlobal(final String contextName,
      final String operationName,
      Table<P> globalTable, Int2IntMap rotateMap,
      DataMap dataMap, Workers workers,
      ResourcePool resourcePool) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    final int selfID = workers.getSelfID();
    final int destID = rotateMap.get(selfID);
    // ---------------------------------------------------
    if (selfID != destID) {
      return rotate(contextName, operationName,
        globalTable, selfID, destID, dataMap,
        workers, resourcePool);
    } else {
      return true;
    }
  }

  // public static long commTime = 0L;
  //
  // public static long getCommTime() {
  // long time = commTime;
  // commTime = 0L;
  // return time;
  // }

  private static <P extends Partition> boolean
    rotate(final String contextName,
      final String operationName,
      Table<P> globalTable, int selfID,
      int destID, DataMap dataMap,
      Workers workers, ResourcePool resourcePool) {
    TableType tableType =
      PartitionUtils.getTableType(globalTable);
    Partition[] ownedPartitions =
      PartitionUtils
        .getPartitionArray(globalTable);
    PartitionType partitionType =
      PartitionUtils
        .getPartitionType(ownedPartitions[0]);
    PartitionCount pCount =
      resourcePool
        .getWritableObject(PartitionCount.class);
    pCount.setWorkerID(selfID);
    pCount
      .setPartitionCount(ownedPartitions.length);
    // Send partition distribution
    Communication.send(contextName, selfID,
      operationName,
      Constants.UNKNOWN_PARTITION_ID,
      DataType.STRUCT_OBJECT, pCount, true,
      false, destID, workers, resourcePool, true);
    resourcePool.releaseWritableObject(pCount);
    pCount = null;
    // Send partitions
    // LOG.info("START SHIFT");
    shift(contextName, operationName,
      ownedPartitions, partitionType, selfID,
      destID, Constants.SEND_DECODE, workers,
      resourcePool);
    // LOG.info("END SHIFT");
    PartitionUtils.releasePartitions(
      resourcePool, ownedPartitions,
      partitionType);
    globalTable.getPartitionMap().clear();
    // ---------------------------------------------
    // Receive partitions
    int numRecvPartitions = -1;
    int i = 0;
    PartitionType recvPartitionType =
      PartitionType.UNKNOWN_PARTITION;
    while (true) {
      Data tmpData =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      if (tmpData != null) {
        if (tmpData.getBodyType() == DataType.STRUCT_OBJECT) {
          PartitionCount recvPCount =
            (PartitionCount) tmpData.getBody();
          numRecvPartitions =
            recvPCount.getPartitionCount();
          resourcePool
            .releaseWritableObject(recvPCount);
          recvPCount = null;
        } else {
          LinkedList<Partition> recvPartitions =
            (LinkedList<Partition>) tmpData
              .getBody();
          if (recvPartitionType == PartitionType.UNKNOWN_PARTITION) {
            recvPartitionType =
              PartitionUtils
                .getPartitionType(recvPartitions
                  .getFirst());
          }
          PartitionUtils.addPartitionsToTable(
            recvPartitions, recvPartitionType,
            globalTable, tableType, resourcePool);
          i += tmpData.getPartitionID();
        }
        if (i == numRecvPartitions) {
          break;
        }
      } else {
        PartitionUtils.releaseTable(resourcePool,
          globalTable, partitionType);
        return false;
      }
    }
    return true;
  }

  private static void shift(String contextName,
    String operationName,
    Partition[] ownedPartitions,
    PartitionType partitionType, int selfID,
    int destID, byte cmd, Workers workers,
    ResourcePool resourcePool) {
    final int numPartitions =
      ownedPartitions.length;
    LinkedList<Partition> sendPartitions =
      new LinkedList<>();
    int partitionSize = 1;
    int partitionCount = 0;
    for (Partition partition : ownedPartitions) {
      sendPartitions.add(partition);
      partitionSize +=
        PartitionUtils.getPartitionSizeInBytes(
          partitionType, partition, resourcePool);
      partitionCount++;
      if ((partitionCount == numPartitions)
        || (partitionSize > Constants.MAX_PARTITION_SEND_BYTE_SIZE)) {
        Data data =
          new Data(DataType.PARTITION_LIST,
            contextName, selfID, operationName,
            sendPartitions.size(), sendPartitions);
        DataSender sender =
          new DataSender(data, destID, workers,
            resourcePool, cmd);
        // long t1 = System.currentTimeMillis();
        sender.execute();
        // long t2 = System.currentTimeMillis();
        // commTime += (t2 - t1);
        data.releaseHeadArray(resourcePool);
        data.releaseEncodedBody(resourcePool);
        data = null;
        sendPartitions.clear();
        partitionSize = 1;
      }
    }
  }
}
