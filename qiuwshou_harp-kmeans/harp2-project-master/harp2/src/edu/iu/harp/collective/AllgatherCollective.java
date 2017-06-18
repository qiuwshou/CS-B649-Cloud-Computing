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

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.iu.harp.array.ArrPartition;
import edu.iu.harp.array.ArrTable;
import edu.iu.harp.array.DoubleArrPlus;
import edu.iu.harp.client.DataSender;
import edu.iu.harp.comm.Communication;
import edu.iu.harp.compute.Computation;
import edu.iu.harp.io.ConnectionPool;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.DataType;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.io.IOUtils;
import edu.iu.harp.message.AllGather;
import edu.iu.harp.message.PartitionCount;
import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.PartitionDecodeTask;
import edu.iu.harp.partition.PartitionType;
import edu.iu.harp.partition.PartitionUtils;
import edu.iu.harp.partition.Table;
import edu.iu.harp.partition.TableType;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.server.reuse.Server;
import edu.iu.harp.trans.DoubleArray;
import edu.iu.harp.worker.Workers;

public class AllgatherCollective {

  private static final Logger LOG = Logger
    .getLogger(AllgatherCollective.class);

  public static void main(String args[])
    throws Exception {
    String driverHost = args[0];
    int driverPort = Integer.parseInt(args[1]);
    int workerID = Integer.parseInt(args[2]);
    long jobID = Long.parseLong(args[3]);
    int partitionByteSize =
      Integer.parseInt(args[4]);
    int numPartitions = Integer.parseInt(args[5]);
    Driver.initLogger(workerID);
    LOG.info("args[] " + driverHost + " "
      + driverPort + " " + workerID + " " + jobID
      + " " + partitionByteSize + " "
      + numPartitions);
    // ------------------------------------------------
    // Worker initialize
    EventQueue eventQueue = new EventQueue();
    DataMap dataMap = new DataMap();
    Workers workers = new Workers(workerID);
    ResourcePool resourcePool =
      new ResourcePool();
    Server server =
      new Server(workers.getSelfInfo().getNode(),
        workers.getSelfInfo().getPort(),
        Constants.NUM_RECV_THREADS, eventQueue,
        dataMap, workers, resourcePool);
    server.start();
    String contextName = jobID + "";
    // Barrier guarantees the living workers get
    // the same view of the barrier result
    boolean isSuccess =
      Communication
        .barrier(contextName, "barrier", dataMap,
          workers, resourcePool);
    LOG.info("Barrier: " + isSuccess);
    // -----------------------------------------------
    // Generate data partition
    ArrTable<DoubleArray> table =
      new ArrTable<>(new DoubleArrPlus());
    int doublesSize = partitionByteSize / 8;
    if (doublesSize < 2) {
      doublesSize = 2;
    }
    // Generate partition data
    for (int i = 0; i < numPartitions; i++) {
      double[] doubles =
        resourcePool.getDoubles(doublesSize);
      doubles[0] = 1; // One row
      for (int j = 1; j < doublesSize; j++) {
        doubles[j] = workerID;
      }
      DoubleArray doubleArray =
        new DoubleArray(doubles, 0, doublesSize);
      // The range of partition ids is based on
      // workerID
      ArrPartition<DoubleArray> partition =
        new ArrPartition<DoubleArray>(workerID
          * numPartitions + i, doubleArray);
      LOG.info("Data Generate, WorkerID: "
        + workerID + " Partition: "
        + partition.getPartitionID()
        + " Row count: " + doubles[0]
        + " First element: " + doubles[1]
        + " Last element: "
        + doubles[doublesSize - 1]);
      table.addPartition(partition);
    }
    // -------------------------------------------------
    // AllGather
    try {
      allgather(contextName, "allgather", table,
        dataMap, workers, resourcePool,
        Constants.NUM_THREADS);
    } catch (Exception e) {
      LOG.error("Fail to allgather", e);
    }
    for (ArrPartition<DoubleArray> partition : table
      .getPartitions()) {
      double[] doubles =
        partition.getArray().getArray();
      int size = partition.getArray().getSize();
      LOG.info(" Partition: "
        + partition.getPartitionID()
        + " Row count: " + doubles[0]
        + " First element: " + doubles[1]
        + " Last element: " + doubles[size - 1]);
    }
    // ---------------------------------------------------
    Driver.reportToDriver(contextName,
      "report-to-driver", workers.getSelfID(),
      driverHost, driverPort, resourcePool);
    ConnectionPool.closeAllConncetions();
    server.stop();
    System.exit(0);
  }

  public static <P extends Partition> boolean
    allgatherLarge(final String contextName,
      final String operationName,
      final Table<P> table,
      final DataMap dataMap,
      final Workers workers,
      final ResourcePool resourcePool,
      final int numDecodeTasks) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    int selfID = workers.getSelfID();
    final int masterID = workers.getMasterID();
    // --------------------------------------------
    LOG.info("Gather partition count.");
    PartitionCount pCount =
      resourcePool
        .getWritableObject(PartitionCount.class);
    pCount.setWorkerID(selfID);
    pCount.setPartitionCount(table
      .getNumPartitions());
    LinkedList<PartitionCount> recvPCounts = null;
    if (workers.isMaster()) {
      recvPCounts = new LinkedList<>();
    }
    boolean isSuccess =
      Communication.structObjGather(contextName,
        operationName + ".gather", pCount,
        recvPCounts, masterID, dataMap, workers,
        resourcePool);
    dataMap.cleanOperationData(contextName,
      operationName + ".gather", resourcePool);
    LOG.info("Partition count is gathered");
    if (workers.isMaster()) {
      if (!isSuccess) {
        resourcePool
          .releaseWritableObject(pCount);
        return false;
      } else {
        // Gather only receive data sent from
        // other workers
        recvPCounts.add(pCount);
      }
    } else {
      resourcePool.releaseWritableObject(pCount);
      if (!isSuccess) {
        return false;
      }
    }
    // ---------------------------------------------------
    AllGather allgather = null;
    LinkedList<AllGather> recvAllGather = null;
    if (workers.isMaster()) {
      allgather =
        resourcePool
          .getWritableObject(AllGather.class);
      while (!recvPCounts.isEmpty()) {
        PartitionCount recvPCount =
          recvPCounts.removeFirst();
        LOG.info("Worker ID: "
          + recvPCount.getWorkerID()
          + ", Partition Count: "
          + recvPCount.getPartitionCount());
        allgather.addPartitionNum(recvPCount
          .getPartitionCount());
        resourcePool
          .releaseWritableObject(recvPCount);
      }
      // Release recvPDistrs
      recvAllGather = null;
      LOG.info("Total Partitions in AllGather: "
        + allgather.getNumPartitions());
    } else {
      recvAllGather = new LinkedList<>();
    }
    LOG.info("Bcast allgather information.");
    isSuccess =
      Communication.mstBcastAndRecv(contextName,
        masterID, operationName + ".bcast",
        Constants.UNKNOWN_PARTITION_ID,
        allgather, true, false, recvAllGather,
        workers, resourcePool, dataMap);
    dataMap.cleanOperationData(contextName,
      operationName + ".bcast", resourcePool);
    LOG.info("Allgather information is bcasted.");
    if (workers.isMaster()) {
      if (!isSuccess) {
        resourcePool
          .releaseWritableObject(allgather);
        return false;
      }
    } else {
      if (!isSuccess) {
        return false;
      } else {
        allgather = recvAllGather.removeFirst();
        recvAllGather = null;
      }
    }
    // -----------------------------------------------
    int numPartitions =
      allgather.getNumPartitions();
    resourcePool.releaseWritableObject(allgather);
    return allgatherLarge(contextName,
      operationName, table, numPartitions,
      dataMap, workers, resourcePool,
      numDecodeTasks);
  }

  public static <P extends Partition> boolean
    allgatherLarge(final String contextName,
      final String operationName,
      final Table<P> table,
      final int numPartitions,
      final DataMap dataMap,
      final Workers workers,
      final ResourcePool resourcePool,
      final int numDecodeTasks) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    List<Partition> ownedPartitions =
      new LinkedList<>(table.getPartitions());
    /*
     * Notice that when sending tuple partitions
     * in regroup,We split a tuple partition to
     * byte arrays and count each of them as one
     * partition. But because we send them in
     * MULTI_PARTITION, we still count these byte
     * array partitions as one partition. However,
     * after decoding, we will have more
     * partitions than the number we set.
     */
    int numOwnedPartitions =
      table.getNumPartitions();
    // Get worker info
    int selfID = workers.getSelfID();
    int nextID = workers.getNextID();
    // LOG.info("Number of Partitions: "
    // + numOwnedPartitions + " " + nextID);
    // Send owned partitions
    if (numOwnedPartitions > 0) {
      Data data = null;
      // For sending multiple partitions,
      // we use partition ID to represent the
      // partition count
      data =
        new Data(DataType.PARTITION_LIST,
          contextName, selfID, operationName,
          numOwnedPartitions, ownedPartitions);
      DataSender sender =
        new DataSender(data, nextID, workers,
          resourcePool, Constants.SEND);
      sender.execute();
      // Release
      data.releaseHeadArray(resourcePool);
      data.releaseEncodedBody(resourcePool);
    }
    // LOG.info("Partitions are sent");
    // New computation and tasks
    List<PartitionDecodeTask> tasks =
      new LinkedList<>();
    for (int i = 0; i < numDecodeTasks; i++) {
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
    for (int i = numOwnedPartitions; i < numPartitions;) {
      // Wait data
      data =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      if (data == null) {
        isFailed = true;
        break;
      }
      recvID = data.getWorkerID();
      // if (data.getBodyType() ==
      // DataType.MULTI_PARTITIONS)
      i += data.getPartitionID();
      // LOG.info("Get partition from worker "
      // + recvID + ". Partitions received: " + i
      // + " total partitions: " + numPartitions);
      // Continue sending to your next neighbor
      if (recvID != nextID) {
        DataSender sender =
          new DataSender(data, nextID, workers,
            resourcePool, Constants.SEND);
        sender.execute();
      }
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
          for (Partition partition : partitions) {
            PartitionUtils.addPartitionToTable(
              partition, table, resourcePool);
          }
          recvData.releaseHeadArray(resourcePool);
          recvData.releaseBodyArray(resourcePool);
        }
      } while (output != null);
    }
    return !isFailed;
  }

  public static <P extends Partition> boolean
    allgather(final String contextName,
      final String operationName,
      final Table<P> table,
      final DataMap dataMap,
      final Workers workers,
      final ResourcePool resourcePool,
      final int numDecodeTasks) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    TableType tableType =
      PartitionUtils.getTableType(table);
    LinkedList<Partition> ownedPartitions =
      new LinkedList<>(table.getPartitions());
    /*
     * Notice that when sending tuple partitions
     * in regroup,We split a tuple partition to
     * byte arrays and count each of them as one
     * partition. But because we send them in
     * MULTI_PARTITION, we still count these byte
     * array partitions as one partition. However,
     * after decoding, we will have more
     * partitions than the number we set.
     */
    int numOwnedPartitions =
      table.getNumPartitions();
    // Get worker info
    int selfID = workers.getSelfID();
    int nextID = workers.getNextID();
    int numWorkers = workers.getNumWorkers();
    // LOG.info("Number of Partitions: "
    // + numOwnedPartitions + " " + nextID);
    // Send owned partitions
    // For sending multiple partitions,
    // we use partition ID to represent the
    // partition count
    Data data =
      new Data(DataType.PARTITION_LIST,
        contextName, selfID, operationName,
        numOwnedPartitions, ownedPartitions);
    DataSender sender =
      new DataSender(data, nextID, workers,
        resourcePool, Constants.SEND);
    sender.execute();
    // Release
    data.releaseHeadArray(resourcePool);
    data.releaseEncodedBody(resourcePool);
    // LOG.info("Partitions are sent");
    // New computation and tasks
    List<PartitionDecodeTask> tasks =
      new LinkedList<>();
    for (int i = 0; i < numDecodeTasks; i++) {
      tasks.add(new PartitionDecodeTask(
        resourcePool));
    }
    Computation<PartitionDecodeTask> compute =
      new Computation<>(tasks);
    compute.start();
    // Receive starts
    int recvID = Constants.UNKNOWN_WORKER_ID;
    boolean isFailed = false;
    data = null;
    for (int i = 1; i < numWorkers; i++) {
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
      if (recvID != nextID) {
        // LOG.info("Send data from " + recvID
        // + " to " + nextID);
        sender =
          new DataSender(data, nextID, workers,
            resourcePool, Constants.SEND);
        sender.execute();
      }
      // The body is not decoded yet
      // Submit for decoding
      compute.submit(data);
    }
    compute.stop();
    Object output = null;
    if (isFailed) {
      do {
        output = compute.waitOutput();
        if (output != null) {
          ((Data) output).release(resourcePool);
        }
      } while (output != null);
    } else {
      do {
        // LOG.info("Wait on the output");
        output = compute.waitOutput();
        if (output != null) {
          Data recvData = (Data) output;
          // LOG.info("Get the output "
          // + recvData.getHeadStatus() + " "
          // + recvData.getBodyStatus());
          LinkedList<Partition> partitions =
            (LinkedList<Partition>) recvData
              .getBody();
          PartitionType partitionType =
            PartitionUtils
              .getPartitionType(partitions.get(0));
          PartitionUtils.addPartitionsToTable(
            partitions, partitionType, table,
            tableType, resourcePool);
          recvData.releaseHeadArray(resourcePool);
          recvData.releaseBodyArray(resourcePool);
        }
      } while (output != null);
    }
    return !isFailed;
  }
}
