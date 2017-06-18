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
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.LinkedList;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.iu.harp.array.ArrPartition;
import edu.iu.harp.array.ArrTable;
import edu.iu.harp.array.DoubleArrPlus;
import edu.iu.harp.comm.Communication;
import edu.iu.harp.io.ConnectionPool;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.io.IOUtils;
import edu.iu.harp.message.PartitionCount;
import edu.iu.harp.message.PartitionDistr;
import edu.iu.harp.message.Regroup;
import edu.iu.harp.partition.EmptyParFunc;
import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.PartitionFunction;
import edu.iu.harp.partition.PartitionType;
import edu.iu.harp.partition.PartitionUtils;
import edu.iu.harp.partition.Table;
import edu.iu.harp.partition.TableType;
import edu.iu.harp.partition.TuplePartition;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.server.reuse.Server;
import edu.iu.harp.trans.DoubleArray;
import edu.iu.harp.worker.Workers;

public class RegroupCollective {
  /** Class logger */
  protected static final Logger LOG = Logger
    .getLogger(RegroupCollective.class);

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
    // ---------------------------------------------------
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
    // ----------------------------------------------------
    // Generate data partition
    ArrTable<DoubleArray> table =
      new ArrTable<DoubleArray>(
        new DoubleArrPlus());
    int doublesSize = partitionByteSize / 8;
    if (doublesSize < 2) {
      doublesSize = 2;
    }
    LOG.info("Double size: " + doublesSize);
    // Generate partition data
    // Assuming count is 1
    for (int i = 0; i < numPartitions; i++) {
      double[] doubles =
        resourcePool.getDoubles(doublesSize);
      doubles[0] = 1; // count is 1
      double num = Math.random() * 100;
      for (int j = 1; j < doublesSize; j++) {
        doubles[j] = num;
      }
      DoubleArray doubleArray =
        new DoubleArray(doubles, 0, doublesSize);
      ArrPartition<DoubleArray> partition =
        new ArrPartition<DoubleArray>(i,
          doubleArray);
      table.addPartition(partition);
      LOG.info("Data Generate, WorkerID: "
        + workerID + " Partition: "
        + partition.getPartitionID() + " Count: "
        + doubles[0] + " First element: "
        + doubles[1] + " Last element: "
        + doubles[doublesSize - 1]);
    }
    // -------------------------------------------------
    // Regroup
    long startTime = System.currentTimeMillis();
    regroupCombine(contextName, "regroup", table,
      dataMap, workers, resourcePool);
    long endTime = System.currentTimeMillis();
    LOG.info("AllReduce time: "
      + (endTime - startTime));
    for (ArrPartition<DoubleArray> partition : table
      .getPartitions()) {
      double[] doubles =
        partition.getArray().getArray();
      int size = partition.getArray().getSize();
      LOG.info(" Partition: "
        + partition.getPartitionID() + " Count: "
        + doubles[0] + " First element: "
        + doubles[1] + " Last element: "
        + doubles[size - 1]);
    }
    // ------------------------------------------------
    Driver.reportToDriver(contextName,
      "report-to-driver", workers.getSelfID(),
      driverHost, driverPort, resourcePool);
    ConnectionPool.closeAllConncetions();
    server.stop();
    System.exit(0);
  }

  public static <P extends Partition> boolean
    regroupCombine(final String contextName,
      final String operationName,
      final Table<P> table,
      final DataMap dataMap,
      final Workers workers,
      final ResourcePool resourcePool) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    final int selfID = workers.getSelfID();
    final int masterID = workers.getMasterID();
    final int numWorkers =
      workers.getNumWorkers();
    // ----------------------------------------------------
    // Gather the information of generated
    // partitions to master
    // Generate partition and worker mapping for
    // regrouping
    // Bcast partition regroup request
    LOG.info("Gather partition information.");
    LinkedList<PartitionDistr> recvPDistrs = null;
    if (workers.isMaster()) {
      recvPDistrs = new LinkedList<>();
    }
    boolean isSuccess =
      PartitionUtils.gatherPartitionDistribution(
        contextName, operationName + ".gather",
        table, recvPDistrs, dataMap, workers,
        resourcePool);
    LOG
      .info("Partition information is gathered.");
    if (!isSuccess) {
      return false;
    }
    // ----------------------------------------------------------
    Regroup regroup = null;
    LinkedList<Regroup> recvRegroup = null;
    if (workers.isMaster()) {
      regroup =
        resourcePool
          .getWritableObject(Regroup.class);
      int destWorkerID = 0;
      while (!recvPDistrs.isEmpty()) {
        PartitionDistr recvPDistr =
          recvPDistrs.removeFirst();
        Int2IntOpenHashMap pDistrMap =
          recvPDistr.getParDistr();
        for (Int2IntMap.Entry entry : pDistrMap
          .int2IntEntrySet()) {
          destWorkerID =
            entry.getIntKey() % numWorkers;
          regroup.addPartitionToWorker(
            entry.getIntKey(), destWorkerID);
          regroup.addWorkerPartitionCount(
            destWorkerID, entry.getIntValue());
        }
        resourcePool
          .releaseWritableObject(recvPDistr);
      }
      // Release recvPDistrs
      recvPDistrs = null;
      // Print regroup partition distribution
      // LOG.info("Partition : Worker");
      // for (Int2IntMap.Entry entry : regroup
      // .getPartitionToWorkerMap()
      // .int2IntEntrySet()) {
      // LOG.info(entry.getIntKey() + " "
      // + entry.getIntValue());
      // }
      // LOG.info("Worker : PartitionCount");
      // for (Int2IntMap.Entry entry : regroup
      // .getWorkerPartitionCountMap()
      // .int2IntEntrySet()) {
      // LOG.info(entry.getIntKey() + " "
      // + entry.getIntValue());
      // }
    } else {
      recvRegroup = new LinkedList<>();
    }
    LOG.info("Bcast regroup information.");
    isSuccess =
      Communication.mstBcastAndRecv(contextName,
        masterID, operationName + ".bcast",
        Constants.UNKNOWN_PARTITION_ID, regroup,
        true, false, recvRegroup, workers,
        resourcePool, dataMap);
    LOG.info("Regroup information is bcasted.");
    dataMap.cleanOperationData(contextName,
      operationName + ".bcast", resourcePool);
    if (workers.isMaster()) {
      if (!isSuccess) {
        resourcePool
          .releaseWritableObject(regroup);
        return false;
      }
    } else {
      if (!isSuccess) {
        return false;
      } else {
        regroup = recvRegroup.removeFirst();
        recvRegroup = null;
      }
    }
    // ------------------------------------------------------
    // Send partition
    LinkedList<Integer> rmPartitionIDs =
      new LinkedList<>();
    int nextPartitionID = -1;
    int localParCount = 0;
    int destWorkerID = -1;
    Random random =
      new Random(System.nanoTime() / 10000
        * 10000 + selfID);
    IntArrayList sendList =
      new IntArrayList(table.getPartitionIDs());
    while (!sendList.isEmpty()) {
      nextPartitionID =
        sendList.removeInt(random
          .nextInt(sendList.size()));
      destWorkerID =
        regroup.getPartitionToWorkerMap().get(
          nextPartitionID);
      // LOG.info("Send " + nextPartitionID +
      // " to "
      // + destWorkerID);
      P partition =
        table.getPartition(nextPartitionID);
      if (destWorkerID == selfID) {
        if (partition instanceof TuplePartition) {
          // finalization should have been done
          // when setting the partition
          // distribution
          TuplePartition tuplePartition =
            ((TuplePartition) partition);
          tuplePartition
            .finalizeByteArrays(resourcePool);
          localParCount +=
            tuplePartition.getByteArrays().size();
        } else {
          localParCount += 1;
        }
      } else {
        rmPartitionIDs.add(nextPartitionID);
        PartitionUtils.sendPartition(contextName,
          selfID, operationName, partition,
          destWorkerID, workers, resourcePool,
          true);
      }
    }
    // ----------------------------------------------------
    // Receive all the partitions
    int numRecvPartitions =
      regroup.getWorkerPartitionCountMap().get(
        selfID)
        - localParCount;
    // LOG.info("Total receive: "
    // + numRecvPartitions);
    // Release regroup
    resourcePool.releaseWritableObject(regroup);
    return PartitionUtils.receivePartitions(
      contextName, operationName, table,
      numRecvPartitions, rmPartitionIDs, dataMap,
      resourcePool);
  }

  public static
    <P extends Partition, PF extends PartitionFunction<P>>
    void applyPartitionFunction(Table<P> table,
      PF function) throws Exception {
    if (!(function instanceof EmptyParFunc)) {
      for (P partition : table.getPartitions()) {
        function.apply(partition);
      }
    }
  }

  public static
    <P extends Partition, PF extends PartitionFunction<P>>
    boolean regroupAggregate(
      final String contextName,
      final String operationName,
      final Table<P> table, final PF function,
      final DataMap dataMap,
      final Workers workers,
      final ResourcePool resourcePool)
      throws Exception {
    boolean isSuccess =
      regroupCombine(contextName, operationName,
        table, dataMap, workers, resourcePool);
    if (!isSuccess) {
      return false;
    }
    applyPartitionFunction(table, function);
    return true;
  }

  public static <P extends Partition> boolean
    regroupCombineLarge(final String contextName,
      final String operationName,
      final Table<P> table, final int numTasks,
      final DataMap dataMap,
      final Workers workers,
      final ResourcePool resourcePool) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    final int selfID = workers.getSelfID();
    final int numWorkers =
      workers.getNumWorkers();
    // -----------------------------------------
    // LOG.info("Gather partition information.");
    // Create a partition map for all partitions
    // in the table distributed on all the workers
    // The actual partition map is unknown,
    // so the map is generated for regrouping
    TableType tableType =
      PartitionUtils.getTableType(table);
    Partition[] partitions =
      PartitionUtils.getPartitionArray(table);
    PartitionType partitionType =
      PartitionUtils
        .getPartitionType(partitions[0]);
    Int2IntOpenHashMap partitionMap =
      new Int2IntOpenHashMap();
    partitionMap.defaultReturnValue(-1);
    LinkedList<PartitionCount> recvPCounts =
      new LinkedList<>();
    int numMatchedPartitions =
      PartitionUtils.regroupPartitionCount(
        contextName, operationName
          + ".regroup.meta", partitions,
        tableType, recvPCounts, partitionMap,
        true, selfID, numWorkers, dataMap,
        workers, resourcePool);
    if (numMatchedPartitions == -1) {
      return false;
    }
    // ----------------------------------------
    int numRecvPartitions = 0;
    for (PartitionCount pCount : recvPCounts) {
      if (pCount.getWorkerID() != selfID) {
        numRecvPartitions +=
          pCount.getPartitionCount();
      }
      resourcePool.releaseWritableObject(pCount);
    }
    recvPCounts = null;
    // ----------------------------------------
    // Send partition
    LinkedList<Partition>[] sendPartitionMap =
      new LinkedList[numWorkers];
    int numSendWorkers = 0;
    IntArrayList rmPartitionIDs =
      new IntArrayList();
    for (P partition : table.getPartitions()) {
      final int partitionID =
        partition.getPartitionID();
      final int workerID =
        partitionID % numWorkers;
      if (workerID != selfID) {
        if (sendPartitionMap[workerID] == null) {
          sendPartitionMap[workerID] =
            new LinkedList<>();
          numSendWorkers++;
        }
        sendPartitionMap[workerID].add(partition);
        rmPartitionIDs.add(partitionID);
      }
    }
    if (numSendWorkers > 0) {
      LocalGlobalSyncCollective.dispatch(
        contextName, operationName,
        sendPartitionMap, numSendWorkers, selfID,
        numTasks, workers, resourcePool);
    }
    LinkedList<Partition> recvPartitionList =
      new LinkedList<>();
    PartitionType recvPartitionType =
      PartitionType.UNKNOWN_PARTITION;
    for (int i = 0; i < numRecvPartitions;) {
      Data data =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      if (data != null) {
        LinkedList<Partition> recvPartitions =
          (LinkedList<Partition>) data.getBody();
        if (recvPartitionType == PartitionType.UNKNOWN_PARTITION) {
          recvPartitionType =
            PartitionUtils
              .getPartitionType(recvPartitions
                .getFirst());
        }
        recvPartitionList.addAll(recvPartitions);
        i += data.getPartitionID();
      } else {
        PartitionUtils.releasePartitions(
          resourcePool, recvPartitionList,
          recvPartitionType);
        return false;
      }
    }
    for (int partitionID : rmPartitionIDs) {
      P partition =
        table.removePartition(partitionID);
      PartitionUtils.releasePartition(
        resourcePool, partition, partitionType);
    }
    LOG.info("Start add partition");
    LocalGlobalSyncCollective
      .addPartitionsToTable(recvPartitionList,
        recvPartitionType, table, tableType,
        numWorkers, numTasks, resourcePool);
    return true;
  }
}
