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
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.iu.harp.comm.Communication;
import edu.iu.harp.graph.EdgeTable;
import edu.iu.harp.graph.EdgeVal;
import edu.iu.harp.graph.InEdgeTable;
import edu.iu.harp.graph.IntMsgVal;
import edu.iu.harp.graph.LongVertexID;
import edu.iu.harp.graph.MsgTable;
import edu.iu.harp.graph.MsgVal;
import edu.iu.harp.graph.NullEdgeVal;
import edu.iu.harp.graph.VertexID;
import edu.iu.harp.io.ConnectionPool;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.message.AllToAll;
import edu.iu.harp.message.PartitionDistr;
import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.PartitionFunction;
import edu.iu.harp.partition.PartitionUtils;
import edu.iu.harp.partition.Table;
import edu.iu.harp.partition.TuplePartition;
import edu.iu.harp.primitivekv.DoubleVNoUpdateCombiner;
import edu.iu.harp.primitivekv.IntVCombiner;
import edu.iu.harp.primitivekv.Long2DoubleKVPartition;
import edu.iu.harp.primitivekv.Long2DoubleKVTable;
import edu.iu.harp.primitivekv.Long2IntKVPartition;
import edu.iu.harp.primitivekv.Long2IntKVTable;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.server.reuse.Server;
import edu.iu.harp.trans.StructObject;
import edu.iu.harp.worker.Workers;

public class GraphCollective {

  private static final Logger LOG = Logger
    .getLogger(GraphCollective.class);

  public static void main(String args[])
    throws Exception {
    String driverHost = args[0];
    int driverPort = Integer.parseInt(args[1]);
    int workerID = Integer.parseInt(args[2]);
    long jobID = Long.parseLong(args[3]);
    int iteration = Integer.parseInt(args[4]);
    Driver.initLogger(workerID);
    LOG.info("args[] " + driverHost + " "
      + driverPort + " " + workerID + " " + jobID
      + " " + iteration);
    // ------------------------------------------------
    // Worker initialize
    EventQueue dataQueue = new EventQueue();
    DataMap dataMap = new DataMap();
    Workers workers = new Workers(workerID);
    ResourcePool resourcePool =
      new ResourcePool();
    Server server =
      new Server(workers.getSelfInfo().getNode(),
        workers.getSelfInfo().getPort(),
        Constants.NUM_RECV_THREADS, dataQueue,
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
    // Generate in-edge table and vertex table
    // (for initial page-rank value) , (for out
    // edge count)
    int vtxCountPerWorker = 5;
    int edgeCountPerWorker = 3;
    int numWorkers = workers.getNumWorkers();
    int totalVtxCount =
      vtxCountPerWorker * numWorkers;
    // Partition seed needs to be the same
    // across all graph tables
    int partitionSeed = numWorkers;
    LOG.info("Total vtx count: " + totalVtxCount);
    InEdgeTable<LongVertexID, NullEdgeVal> inEdgeTable =
      new InEdgeTable<>(LongVertexID.class,
        NullEdgeVal.class, 16, partitionSeed,
        resourcePool);
    long source = 0;
    long target = 0;
    Random random = new Random(workerID);
    for (int i = 0; i < edgeCountPerWorker; i++) {
      target = random.nextInt(totalVtxCount);
      do {
        source = random.nextInt(totalVtxCount);
      } while (source == target);
      LOG.info("Edge: "
        + source
        + "->"
        + target
        + " "
        + inEdgeTable.addEdge(new LongVertexID(
          source), new NullEdgeVal(),
          new LongVertexID(target)));
    }
    // --------------------------------------------------
    // Regroup edges
    try {
      LOG.info("PRINT EDGE TABLE START");
      printEdgeTable(inEdgeTable);
      LOG.info("PRINT EDGE TABLE END");
      LOG.info("REGROUP START");
      isSuccess =
        RegroupCollective.regroupCombine(
          contextName, "regroup-edges",
          inEdgeTable, dataMap, workers,
          resourcePool);
      LOG.info("REGROUP End " + isSuccess);
      LOG.info("PRINT EDGE TABLE START");
      printEdgeTable(inEdgeTable);
      LOG.info("PRINT EDGE TABLE END");
    } catch (Exception e) {
      LOG.error("Error when adding edges", e);
    }
    // --------------------------------------------------
    // Initialize page rank of each vtx
    Long2DoubleKVTable ldVtxTable =
      new Long2DoubleKVTable(
        new DoubleVNoUpdateCombiner(),
        vtxCountPerWorker, partitionSeed,
        resourcePool);
    try {
      for (TuplePartition partition : inEdgeTable
        .getPartitions()) {
        partition.defaultReadPos();
        while (partition.readTuple()) {
          StructObject[] tuple =
            partition.getTuple();
          LongVertexID targetID =
            (LongVertexID) tuple[2];
          ldVtxTable.addKeyVal(
            targetID.getVertexID(),
            1.0 / (double) totalVtxCount);
        }
      }
    } catch (Exception e) {
      LOG
        .error(
          "Fail to create vertex table from the edge table.",
          e);
    }
    LOG.info("PRINT VTX TABLE START");
    printVtxTable(ldVtxTable);
    LOG.info("PRINT VTX TABLE END");
    // --------------------------------------------------
    // Generate message table for counting out
    // edges
    MsgTable<LongVertexID, IntMsgVal> msgTable =
      new MsgTable<>(LongVertexID.class,
        IntMsgVal.class, 12, partitionSeed,
        resourcePool);
    IntMsgVal iMsgVal = new IntMsgVal(1);
    for (TuplePartition partition : inEdgeTable
      .getPartitions()) {
      partition.defaultReadPos();
      while (partition.readTuple()) {
        StructObject[] tuple =
          partition.getTuple();
        LongVertexID sourceID =
          (LongVertexID) tuple[0];
        msgTable.addMsg(sourceID, iMsgVal);
      }
    }
    LOG.info("PRINT MSG TABLE START");
    printMsgTable(msgTable);
    LOG.info("PRINT MSG TABLE END");
    // All-to-all communication, moves message
    // partition to the place
    // where the vertex partition locate
    LOG.info("All MSG TO ALL VTX START");
    try {
      allToAll(contextName, "send-msg-to-vtx",
        msgTable, ldVtxTable, dataMap, workers,
        resourcePool);
    } catch (Exception e) {
      LOG.error("Error in all msg to all vtx", e);
    }
    LOG.info("All MSG TO ALL VTX END");
    LOG.info("PRINT MSG TABLE START");
    printMsgTable(msgTable);
    LOG.info("PRINT MSG TABLE END");
    // Process message table
    // Another vertex table for out edge count
    Long2IntKVTable liVtxTable =
      new Long2IntKVTable(new IntVCombiner(),
        vtxCountPerWorker, partitionSeed,
        resourcePool);
    try {
      for (TuplePartition partition : msgTable
        .getPartitions()) {
        partition.defaultReadPos();
        while (partition.readTuple()) {
          StructObject[] tuple =
            partition.getTuple();
          LongVertexID vtxID =
            (LongVertexID) tuple[0];
          IntMsgVal msgVal = (IntMsgVal) tuple[1];
          liVtxTable.addKeyVal(
            vtxID.getVertexID(),
            msgVal.getIntMsgVal());
        }
      }
    } catch (Exception e) {
      LOG.error("Error when processing msg.", e);
    }
    LOG.info("PRINT VTX TABLE START");
    printVtxTable(liVtxTable);
    LOG.info("PRINT VTX TABLE END");
    // ----------------------------------------------------
    Driver.reportToDriver(contextName,
      "report-to-driver", workers.getSelfID(),
      driverHost, driverPort, resourcePool);
    ConnectionPool.closeAllConncetions();
    server.stop();
    System.exit(0);
  }

  private static
    <I extends VertexID, E extends EdgeVal, T extends EdgeTable<I, E>>
    void printEdgeTable(T edgeTable) {
    for (TuplePartition partition : edgeTable
      .getPartitions()) {
      partition.defaultReadPos();
      while (partition.readTuple()) {
        StructObject[] tuple =
          partition.getTuple();
        LongVertexID sourceID =
          (LongVertexID) tuple[0];
        LongVertexID targetID =
          (LongVertexID) tuple[2];
        LOG.info("Partiiton ID: "
          + partition.getPartitionID()
          + ", Edge: " + sourceID.getVertexID()
          + "->" + targetID.getVertexID());
      }
    }
  }

  private static
    <I extends VertexID, M extends MsgVal, T extends MsgTable<I, M>>
    void printMsgTable(T msgTable) {
    for (TuplePartition partition : msgTable
      .getPartitions()) {
      partition.defaultReadPos();
      while (partition.readTuple()) {
        StructObject[] tuple =
          partition.getTuple();
        LongVertexID sourceID =
          (LongVertexID) tuple[0];
        IntMsgVal msgVal = (IntMsgVal) tuple[1];
        LOG.info("Partiiton ID: "
          + partition.getPartitionID()
          + ", MSG: " + sourceID.getVertexID()
          + "->" + msgVal.getIntMsgVal());
      }
    }
  }

  private static void printVtxTable(
    Long2DoubleKVTable ldVtxTable) {
    Collection<Long2DoubleKVPartition> vtxPartitions =
      ldVtxTable.getPartitions();
    for (Long2DoubleKVPartition partition : vtxPartitions) {
      for (Long2DoubleMap.Entry entry : partition
        .getKVMap().long2DoubleEntrySet()) {
        LOG.info("Patition: "
          + partition.getPartitionID() + " "
          + entry.getLongKey() + " "
          + entry.getDoubleValue());
      }
    }
  }

  private static void printVtxTable(
    Long2IntKVTable liVtxTable) {
    Collection<Long2IntKVPartition> vtxPartitions =
      liVtxTable.getPartitions();
    for (Long2IntKVPartition partition : vtxPartitions) {
      for (Long2IntMap.Entry entry : partition
        .getKVMap().long2IntEntrySet()) {
        LOG.info(entry.getLongKey() + " "
          + entry.getIntValue());
      }
    }
  }

  public static
    <P1 extends Partition, P2 extends Partition>
    boolean allToAll(final String contextName,
      final String operationName,
      Table<P1> dynamicTable,
      Table<P2> staticTable, DataMap dataMap,
      Workers workers, ResourcePool resourcePool) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    final int selfID = workers.getSelfID();
    final int masterID = workers.getMasterID();
    final int numWorkers =
      workers.getNumWorkers();
    // ---------------------------------------------------
    // Gather the partition information of the
    // static table to master
    LOG
      .info("Gather static partition information.");
    LinkedList<PartitionDistr> recvStaticPDistrs =
      null;
    if (workers.isMaster()) {
      recvStaticPDistrs = new LinkedList<>();
    }
    boolean isSuccess =
      PartitionUtils.gatherPartitionDistribution(
        contextName, operationName
          + ".gatherstatic", staticTable,
        recvStaticPDistrs, dataMap, workers,
        resourcePool);
    LOG
      .info("Static partition information is gathered.");
    if (!isSuccess) {
      return false;
    }
    // ---------------------------------------------------
    // Gather the information of dynamic table
    // partitions to master
    LOG
      .info("Gather dynamic partition information.");
    LinkedList<PartitionDistr> recvDynamicPDistrs =
      null;
    if (workers.isMaster()) {
      recvDynamicPDistrs = new LinkedList<>();
    }
    isSuccess =
      PartitionUtils.gatherPartitionDistribution(
        contextName, operationName
          + ".gatherdynamic", dynamicTable,
        recvDynamicPDistrs, dataMap, workers,
        resourcePool);
    LOG
      .info("Dynamic partition information is gathered.");
    if (!isSuccess) {
      return false;
    }
    // ---------------------------------------------------
    // Generate partition and worker mapping for
    // all-to-one-to-all
    // Bcast partition regroup request
    AllToAll allToAll = null;
    LinkedList<AllToAll> recvAllToAll = null;
    if (workers.isMaster()) {
      // partition <-> workers mapping
      // based on static partition info
      Int2ObjectOpenHashMap<IntArrayList> partitionToWorkerMap =
        new Int2ObjectOpenHashMap<IntArrayList>();
      while (!recvStaticPDistrs.isEmpty()) {
        PartitionDistr recvPDistr =
          recvStaticPDistrs.removeFirst();
        int workerID = recvPDistr.getWorkerID();
        Int2IntOpenHashMap pDistrMap =
          recvPDistr.getParDistr();
        // See which partition is on which workers
        for (Int2IntMap.Entry entry : pDistrMap
          .int2IntEntrySet()) {
          int partitionID = entry.getIntKey();
          IntArrayList destWorkerIDs =
            partitionToWorkerMap.get(partitionID);
          if (destWorkerIDs == null) {
            destWorkerIDs = new IntArrayList();
            partitionToWorkerMap.put(partitionID,
              destWorkerIDs);
          }
          destWorkerIDs.add(workerID);
        }
        resourcePool
          .releaseWritableObject(recvPDistr);
      }
      recvStaticPDistrs = null;
      // worker <-> partition count
      // based on dynamic partition info
      Int2IntOpenHashMap workerPartitionCountMap =
        new Int2IntOpenHashMap();
      while (!recvDynamicPDistrs.isEmpty()) {
        PartitionDistr recvPDistr =
          recvDynamicPDistrs.removeFirst();
        Int2IntOpenHashMap pDistrMap =
          recvPDistr.getParDistr();
        for (Int2IntMap.Entry entry : pDistrMap
          .int2IntEntrySet()) {
          int partitionID = entry.getIntKey();
          IntArrayList destWorkerIDs =
            partitionToWorkerMap.get(partitionID);
          if (destWorkerIDs != null) {
            for (int destID : destWorkerIDs) {
              workerPartitionCountMap.addTo(
                destID, entry.getIntValue());
            }
          } else {
            int altDestWorkerID =
              partitionID % numWorkers;
            /*
             * Because this dynamic partition
             * cannot find worker destination with
             * matched static partition ID, we
             * hash the partition ID to a worker
             * ID, and add it to partition <->
             * worker mapping
             */
            destWorkerIDs = new IntArrayList();
            partitionToWorkerMap.put(partitionID,
              destWorkerIDs);
            destWorkerIDs.add(altDestWorkerID);
            workerPartitionCountMap.addTo(
              altDestWorkerID,
              entry.getIntValue());
          }
        }
        resourcePool
          .releaseWritableObject(recvPDistr);
      }
      recvDynamicPDistrs = null;
      // Print partition distribution
      LOG.info("Partition : Worker");
      StringBuffer buffer = new StringBuffer();
      for (Entry<Integer, IntArrayList> entry : partitionToWorkerMap
        .entrySet()) {
        for (int val : entry.getValue()) {
          buffer.append(val + " ");
        }
        LOG.info(entry.getKey() + " : " + buffer);
        buffer.delete(0, buffer.length());
      }
      LOG.info("Worker : Partition Count");
      for (Entry<Integer, Integer> entry : workerPartitionCountMap
        .entrySet()) {
        LOG.info(entry.getKey() + " "
          + entry.getValue());
      }
      allToAll =
        resourcePool
          .getWritableObject(AllToAll.class);
      allToAll
        .setPartitionToWorkerMap(partitionToWorkerMap);
      allToAll
        .setWorkerPartitionCountMap(workerPartitionCountMap);
    } else {
      recvAllToAll = new LinkedList<>();
    }
    // --------------------------------------------------
    LOG.info("Bcast all-to-all information.");
    isSuccess =
      Communication.mstBcastAndRecv(contextName,
        masterID, operationName + ".bcast",
        Constants.UNKNOWN_PARTITION_ID, allToAll,
        true, false, recvAllToAll, workers,
        resourcePool, dataMap);
    dataMap.cleanOperationData(contextName,
      operationName + ".bcast", resourcePool);
    if (workers.isMaster()) {
      if (!isSuccess) {
        resourcePool
          .releaseWritableObject(allToAll);
        return false;
      }
    } else {
      if (!isSuccess) {
        return false;
      } else {
        allToAll = recvAllToAll.removeFirst();
        recvAllToAll = null;
      }
    }
    LOG.info("Regroup information is bcasted.");
    // -------------------------------------------------
    // Send partition
    LinkedList<Integer> rmPartitionIDs =
      new LinkedList<>();
    int nextPartitionID = -1;
    int localParCount = 0;
    boolean isSentLocal = false;
    IntArrayList destWorkerIDs = null;
    Random random =
      new Random(System.nanoTime() / 10000
        * 10000 + selfID);
    IntArrayList sendList =
      new IntArrayList(
        dynamicTable.getPartitionIDs());
    while (!sendList.isEmpty()) {
      nextPartitionID =
        sendList.removeInt(random
          .nextInt(sendList.size()));
      destWorkerIDs =
        allToAll.getPartitionToWorkerMap().get(
          nextPartitionID);
      P1 partition =
        dynamicTable
          .getPartition(nextPartitionID);
      for (int destID : destWorkerIDs) {
        if (destID == selfID) {
          if (partition instanceof TuplePartition) {
            // Finalization should have been done
            // when setting the partition
            // distribution
            TuplePartition tuplePartition =
              ((TuplePartition) partition);
            tuplePartition
              .finalizeByteArrays(resourcePool);
            localParCount +=
              tuplePartition.getByteArrays()
                .size();
          } else {
            localParCount += 1;
          }
          isSentLocal = true;
        } else {
          PartitionUtils.sendPartition(
            contextName, selfID, operationName,
            partition, destID, workers,
            resourcePool, true);
        }
      }
      // Remove and release
      if (!isSentLocal) {
        rmPartitionIDs.add(nextPartitionID);
      }
      // Reset
      isSentLocal = false;
    }
    // --------------------------------------------------
    // Receive all the partitions
    int numRecvPartitions =
      allToAll.getWorkerPartitionCountMap().get(
        selfID)
        - localParCount;
    LOG.info("Total receive: "
      + numRecvPartitions);
    // Release regroup
    resourcePool.releaseWritableObject(allToAll);
    return PartitionUtils.receivePartitions(
      contextName, operationName, dynamicTable,
      numRecvPartitions, rmPartitionIDs, dataMap,
      resourcePool);
  }

  public static
    <P1 extends Partition, P2 extends Partition, PF extends PartitionFunction<P1>>
    boolean allToOneToAll(
      final String contextName,
      final String operationName,
      final Table<P1> dynamicTable,
      final Table<P2> staticTable,
      final PF function, DataMap dataMap,
      Workers workers, ResourcePool resourcePool) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    final int selfID = workers.getSelfID();
    final int masterID = workers.getMasterID();
    final int numWorkers =
      workers.getNumWorkers();
    // ---------------------------------------------------
    // Gather the partition information of the
    // static table to master
    LOG
      .info("Gather static partition information.");
    LinkedList<PartitionDistr> recvStaticPDistrs =
      null;
    if (workers.isMaster()) {
      recvStaticPDistrs = new LinkedList<>();
    }
    boolean isSuccess =
      PartitionUtils.gatherPartitionDistribution(
        contextName, operationName
          + ".gatherstatic", staticTable,
        recvStaticPDistrs, dataMap, workers,
        resourcePool);
    LOG
      .info("Static partition information is gathered.");
    if (!isSuccess) {
      return false;
    }
    // ---------------------------------------------------
    // Gather the information of dynamic table
    // partitions to master
    LOG
      .info("Gather dynamic partition information.");
    LinkedList<PartitionDistr> recvDynamicPDistrs =
      null;
    if (workers.isMaster()) {
      recvDynamicPDistrs = new LinkedList<>();
    }
    isSuccess =
      PartitionUtils.gatherPartitionDistribution(
        contextName, operationName
          + ".gatherdynamic.stage1",
        dynamicTable, recvDynamicPDistrs,
        dataMap, workers, resourcePool);
    LOG
      .info("Dynamic partition information is gathered.");
    if (!isSuccess) {
      return false;
    }
    // ---------------------------------------------------
    // Generate partition and worker mapping
    // Bcast request
    Int2ObjectOpenHashMap<IntArrayList> partitionToWorkerMap =
      null;
    Int2IntOpenHashMap workerPartitionCountMap =
      null;
    if (workers.isMaster()) {
      // partition <-> workers mapping
      // based on static partition info
      partitionToWorkerMap =
        new Int2ObjectOpenHashMap<>();
      while (!recvStaticPDistrs.isEmpty()) {
        PartitionDistr recvPDistr =
          recvStaticPDistrs.removeFirst();
        int workerID = recvPDistr.getWorkerID();
        Int2IntOpenHashMap pDistrMap =
          recvPDistr.getParDistr();
        // See which partition is on which workers
        for (Int2IntMap.Entry entry : pDistrMap
          .int2IntEntrySet()) {
          int partitionID = entry.getIntKey();
          IntArrayList destWorkerIDs =
            partitionToWorkerMap.get(partitionID);
          if (destWorkerIDs == null) {
            destWorkerIDs = new IntArrayList();
            partitionToWorkerMap.put(partitionID,
              destWorkerIDs);
          }
          destWorkerIDs.add(workerID);
        }
        resourcePool
          .releaseWritableObject(recvPDistr);
      }
      recvStaticPDistrs = null;
      randomOrderDestWorkers(partitionToWorkerMap);
      // worker <-> 1st stage partition count
      // based on dynamic partition info
      workerPartitionCountMap =
        new Int2IntOpenHashMap(numWorkers);
      while (!recvDynamicPDistrs.isEmpty()) {
        PartitionDistr recvPDistr =
          recvDynamicPDistrs.removeFirst();
        Int2IntOpenHashMap pDistrMap =
          recvPDistr.getParDistr();
        for (Int2IntMap.Entry entry : pDistrMap
          .int2IntEntrySet()) {
          int partitionID = entry.getIntKey();
          IntArrayList destWorkerIDs =
            partitionToWorkerMap.get(partitionID);
          if (destWorkerIDs != null) {
            // Add partition count on the first
            // worker, which is used for the first
            // stage of sending
            workerPartitionCountMap.addTo(
              destWorkerIDs.get(0),
              entry.getIntValue());
          } else {
            int altDestWorkerID =
              partitionID % numWorkers;
            /*
             * Because this dynamic partition
             * cannot find worker destination with
             * matched static partition ID, we
             * hash the partition ID to a worker
             * ID, and add it to partition <->
             * worker mapping
             */
            destWorkerIDs = new IntArrayList();
            partitionToWorkerMap.put(partitionID,
              destWorkerIDs);
            destWorkerIDs.add(altDestWorkerID);
            workerPartitionCountMap.put(
              altDestWorkerID,
              entry.getIntValue());
          }
        }
        resourcePool
          .releaseWritableObject(recvPDistr);
      }
      recvDynamicPDistrs = null;
      // Print partition distribution on workers
      // calculated based on the static partition
      // info
      LOG.info("Partition : Worker");
      StringBuffer buffer = new StringBuffer();
      for (Entry<Integer, IntArrayList> entry : partitionToWorkerMap
        .entrySet()) {
        for (int val : entry.getValue()) {
          buffer.append(val + " ");
        }
        LOG.info(entry.getKey() + " : " + buffer);
        buffer.delete(0, buffer.length());
      }
      LOG.info("Worker : Partition Count");
      for (Int2IntMap.Entry entry : workerPartitionCountMap
        .int2IntEntrySet()) {
        LOG.info(entry.getIntKey() + " "
          + entry.getIntValue());
      }
    }
    return allToOneToAllWithDistribution(
      contextName, operationName, dynamicTable,
      function, partitionToWorkerMap,
      workerPartitionCountMap, masterID, selfID,
      numWorkers, dataMap, workers, resourcePool);
  }

  public static
    <P extends Partition, PF extends PartitionFunction<P>>
    boolean allToOneToAll(
      final String contextName,
      final String operationName,
      final Table<P> table, final PF function,
      DataMap dataMap, Workers workers,
      ResourcePool resourcePool) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    final int selfID = workers.getSelfID();
    final int masterID = workers.getMasterID();
    final int numWorkers =
      workers.getNumWorkers();
    // ---------------------------------------------------
    // Gather the partition information of the
    // static table to master
    LOG.info("Gather partition information.");
    LinkedList<PartitionDistr> recvPartitionDistrs =
      null;
    if (workers.isMaster()) {
      recvPartitionDistrs = new LinkedList<>();
    }
    boolean isSuccess =
      PartitionUtils.gatherPartitionDistribution(
        contextName, operationName
          + ".gather.stage1", table,
        recvPartitionDistrs, dataMap, workers,
        resourcePool);
    LOG
      .info("Partition information is gathered.");
    if (!isSuccess) {
      return false;
    }
    // ---------------------------------------------------
    // Generate partition and worker mapping
    // Bcast request
    Int2ObjectOpenHashMap<IntArrayList> partitionToWorkerMap =
      null;
    Int2IntOpenHashMap workerPartitionCountMap =
      null;
    if (workers.isMaster()) {
      // partition <-> workers mapping
      // based on static partition info
      partitionToWorkerMap =
        new Int2ObjectOpenHashMap<>();
      for (PartitionDistr recvPDistr : recvPartitionDistrs) {
        int workerID = recvPDistr.getWorkerID();
        Int2IntOpenHashMap pDistrMap =
          recvPDistr.getParDistr();
        // See which partition is on which workers
        for (Int2IntMap.Entry entry : pDistrMap
          .int2IntEntrySet()) {
          int partitionID = entry.getIntKey();
          IntArrayList destWorkerIDs =
            partitionToWorkerMap.get(partitionID);
          if (destWorkerIDs == null) {
            destWorkerIDs = new IntArrayList();
            partitionToWorkerMap.put(partitionID,
              destWorkerIDs);
          }
          destWorkerIDs.add(workerID);
        }
      }
      randomOrderDestWorkers(partitionToWorkerMap);
      // worker <-> 1st stage partition count
      // based on dynamic partition info
      workerPartitionCountMap =
        new Int2IntOpenHashMap(numWorkers);
      while (!recvPartitionDistrs.isEmpty()) {
        PartitionDistr recvPDistr =
          recvPartitionDistrs.removeFirst();
        Int2IntOpenHashMap pDistrMap =
          recvPDistr.getParDistr();
        for (Int2IntMap.Entry entry : pDistrMap
          .int2IntEntrySet()) {
          int partitionID = entry.getIntKey();
          IntArrayList destWorkerIDs =
            partitionToWorkerMap.get(partitionID);
          // Add partition count on the first
          // worker, which is used for the first
          // stage of sending
          workerPartitionCountMap.addTo(
            destWorkerIDs.get(0),
            entry.getIntValue());
        }
        resourcePool
          .releaseWritableObject(recvPDistr);
      }
      recvPartitionDistrs = null;
      // Print partition distribution on workers
      // calculated based on the static partition
      // info
      // LOG.info("Partition : Worker");
      // StringBuffer buffer = new StringBuffer();
      // for (Entry<Integer, IntArrayList> entry :
      // partitionToWorkerMap
      // .entrySet()) {
      // for (int val : entry.getValue()) {
      // buffer.append(val + " ");
      // }
      // LOG.info(entry.getKey() + " : " +
      // buffer);
      // buffer.delete(0, buffer.length());
      // }
      // LOG.info("Worker : Partition Count");
      // for (Int2IntMap.Entry entry :
      // workerPartitionCountMap
      // .int2IntEntrySet()) {
      // LOG.info(entry.getIntKey() + " "
      // + entry.getIntValue());
      // }
    }
    return allToOneToAllWithDistribution(
      contextName, operationName, table,
      function, partitionToWorkerMap,
      workerPartitionCountMap, masterID, selfID,
      numWorkers, dataMap, workers, resourcePool);
  }

  private static
    void
    randomOrderDestWorkers(
      Int2ObjectOpenHashMap<IntArrayList> partitionToWorkerMap) {
    // Randomize the order of the dest workers
    Random random = new Random(System.nanoTime());
    int randomIndex = -1;
    int workerID = -1;
    for (Entry<Integer, IntArrayList> entry : partitionToWorkerMap
      .entrySet()) {
      IntArrayList destWorkerIDs =
        entry.getValue();
      if (destWorkerIDs.size() > 1) {
        for (int i = destWorkerIDs.size(); i >= 2; i--) {
          randomIndex = random.nextInt(i);
          if ((i - 1) != randomIndex) {
            workerID =
              destWorkerIDs.get(randomIndex);
            destWorkerIDs.set(randomIndex,
              destWorkerIDs.get(i - 1));
            destWorkerIDs.set(i - 1, workerID);
          }
        }
      }
    }
  }

  private static
    <P extends Partition, PF extends PartitionFunction<P>>
    boolean
    allToOneToAllWithDistribution(
      final String contextName,
      final String operationName,
      final Table<P> table,
      final PF function,
      final Int2ObjectOpenHashMap<IntArrayList> partitionToWorkerMap,
      Int2IntOpenHashMap workerPartitionCountMap,
      final int masterID, final int selfID,
      final int numWorkers,
      final DataMap dataMap,
      final Workers workers,
      final ResourcePool resourcePool) {
    AllToAll allToAllStage1 = null;
    LinkedList<AllToAll> recvAllToAllStage1 =
      null;
    if (workers.isMaster()) {
      allToAllStage1 =
        resourcePool
          .getWritableObject(AllToAll.class);
      allToAllStage1
        .setPartitionToWorkerMap(partitionToWorkerMap);
      allToAllStage1
        .setWorkerPartitionCountMap(workerPartitionCountMap);
    } else {
      recvAllToAllStage1 = new LinkedList<>();
    }
    // --------------------------------------------------
    LOG.info("Bcast stage 1 AOA information.");
    boolean isSuccess =
      Communication.mstBcastAndRecv(contextName,
        masterID,
        operationName + ".bcast.stage1",
        Constants.UNKNOWN_PARTITION_ID,
        allToAllStage1, true, false,
        recvAllToAllStage1, workers,
        resourcePool, dataMap);
    dataMap.cleanOperationData(contextName,
      operationName + ".bcast.stage1",
      resourcePool);
    if (workers.isMaster()) {
      if (!isSuccess) {
        resourcePool
          .releaseWritableObject(allToAllStage1);
        return false;
      }
    } else {
      if (!isSuccess) {
        return false;
      } else {
        allToAllStage1 =
          recvAllToAllStage1.removeFirst();
        recvAllToAllStage1 = null;
      }
    }
    LOG
      .info("Stage 1 AOA information is bcasted.");
    // -------------------------------------------------
    // Stage 1: send partitions to the first
    // worker in the worker list
    isSuccess =
      sendAndRecvPartitionsAOAStage1(contextName,
        operationName + ".aoa.stage1", table,
        selfID, allToAllStage1, dataMap, workers,
        resourcePool);
    dataMap
      .cleanOperationData(contextName,
        operationName + ".aoa.stage1",
        resourcePool);
    if (!isSuccess) {
      resourcePool
        .releaseWritableObject(allToAllStage1);
      return false;
    }
    // ---------------------------------------------------
    // Apply partition function
    try {
      RegroupCollective.applyPartitionFunction(
        table, function);
    } catch (Exception e) {
      LOG.error(
        "Fail to apply the partiiton function, "
          + "all-to-one-to-all still continues.",
        e);
    }
    // -------------------------------------------------
    // Stage 2:
    // Gather the dynamic table distribution
    // after applying the function
    LOG
      .info("Gather partition information AOA  stage 2.");
    LinkedList<PartitionDistr> recvPartitionDistrs =
      null;
    if (workers.isMaster()) {
      recvPartitionDistrs = new LinkedList<>();
    }
    isSuccess =
      PartitionUtils.gatherPartitionDistribution(
        contextName, operationName
          + ".gather.stage2", table,
        recvPartitionDistrs, dataMap, workers,
        resourcePool);
    LOG
      .info("Partition information is gathered AOA stage 2.");
    if (!isSuccess) {
      return false;
    }
    // ---------------------------------------------
    // Generate partition and worker mapping
    // Bcast request
    AllToAll allToAllStage2 = null;
    LinkedList<AllToAll> recvAllToAllStage2 =
      null;
    if (workers.isMaster()) {
      workerPartitionCountMap =
        new Int2IntOpenHashMap(numWorkers);
      while (!recvPartitionDistrs.isEmpty()) {
        PartitionDistr recvPDistr =
          recvPartitionDistrs.removeFirst();
        Int2IntOpenHashMap pDistrMap =
          recvPDistr.getParDistr();
        for (Int2IntMap.Entry entry : pDistrMap
          .int2IntEntrySet()) {
          int partitionID = entry.getIntKey();
          IntArrayList destWorkerIDs =
            allToAllStage1
              .getPartitionToWorkerMap().get(
                partitionID);
          for (int i = 1; i < destWorkerIDs
            .size(); i++) {
            workerPartitionCountMap.addTo(
              destWorkerIDs.get(i),
              entry.getIntValue());
          }
        }
        resourcePool
          .releaseWritableObject(recvPDistr);
      }
      recvPartitionDistrs = null;
      // Print
      // LOG.info("Worker : Partition Count");
      // for (Int2IntMap.Entry entry :
      // workerPartitionCountMap
      // .int2IntEntrySet()) {
      // LOG.info(entry.getIntKey() + " "
      // + entry.getIntValue());
      // }
      allToAllStage2 =
        resourcePool
          .getWritableObject(AllToAll.class);
      allToAllStage2
        .setPartitionToWorkerMap(null);
      allToAllStage2
        .setWorkerPartitionCountMap(workerPartitionCountMap);
    } else {
      recvAllToAllStage2 = new LinkedList<>();
    }
    // --------------------------------------------------
    LOG.info("Bcast stage 2 AOA information.");
    isSuccess =
      Communication.mstBcastAndRecv(contextName,
        masterID,
        operationName + ".bcast.stage2",
        Constants.UNKNOWN_PARTITION_ID,
        allToAllStage2, true, false,
        recvAllToAllStage2, workers,
        resourcePool, dataMap);
    dataMap.cleanOperationData(contextName,
      operationName + ".bcast.stage2",
      resourcePool);
    LOG
      .info("Stage 2 AOA information is bcasted: "
        + isSuccess);
    if (workers.isMaster()) {
      if (!isSuccess) {
        resourcePool
          .releaseWritableObject(allToAllStage1);
        return false;
      }
    } else {
      if (!isSuccess) {
        return false;
      } else {
        allToAllStage2 =
          recvAllToAllStage2.removeFirst();
        recvAllToAllStage2 = null;
      }
    }
    // -------------------------------------
    // Send partition in stage 2
    isSuccess =
      sendAndRecvPartitionsAOAStage2(contextName,
        operationName + ".aoa.stage2", table,
        selfID, allToAllStage1, allToAllStage2,
        dataMap, workers, resourcePool);
    dataMap
      .cleanOperationData(contextName,
        operationName + ".aoa.stage2",
        resourcePool);
    resourcePool
      .releaseWritableObject(allToAllStage1);
    resourcePool
      .releaseWritableObject(allToAllStage2);
    return isSuccess;
  }

  private static <P extends Partition> boolean
    sendAndRecvPartitionsAOAStage1(
      final String contextName,
      final String operationName, Table<P> table,
      int selfID, AllToAll allToAllStage1,
      DataMap dataMap, Workers workers,
      ResourcePool resourcePool) {
    LinkedList<Integer> rmPartitionIDs =
      new LinkedList<>();
    int localParCount = 0;
    Random random = new Random(System.nanoTime());
    IntArrayList sendList =
      new IntArrayList(table.getPartitionIDs());
    while (!sendList.isEmpty()) {
      int nextPartitionID =
        sendList.removeInt(random
          .nextInt(sendList.size()));
      P partition =
        table.getPartition(nextPartitionID);
      // Get the first worker ID in the dest list
      int destWorkerID =
        allToAllStage1.getPartitionToWorkerMap()
          .get(nextPartitionID).get(0);
      // LOG
      // .info("Send partition " + nextPartitionID
      // + " to " + destWorkerID);
      if (destWorkerID == selfID) {
        if (partition instanceof TuplePartition) {
          // Finalization should have been done
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
        PartitionUtils.sendPartition(contextName,
          selfID, operationName, partition,
          destWorkerID, workers, resourcePool,
          true);
        rmPartitionIDs.add(nextPartitionID);
      }
    }
    // --------------------------------------------------
    // Receive all the partitions
    int numRecvPartitions =
      allToAllStage1.getWorkerPartitionCountMap()
        .get(selfID) - localParCount;
    LOG.info("Total receive: "
      + numRecvPartitions);
    return PartitionUtils.receivePartitionsAlt(
      contextName, operationName, table,
      numRecvPartitions, rmPartitionIDs, dataMap,
      resourcePool);
  }

  private static <P extends Partition> boolean
    sendAndRecvPartitionsAOAStage2(
      final String contextName,
      final String operationName, Table<P> table,
      int selfID, AllToAll allToAllStage1,
      AllToAll allToAllStage2, DataMap dataMap,
      Workers workers, ResourcePool resourcePool) {
    LinkedList<Integer> rmPartitionIDs =
      new LinkedList<>();
    Random random = new Random(System.nanoTime());
    IntArrayList sendList =
      new IntArrayList(table.getPartitionIDs());
    while (!sendList.isEmpty()) {
      int nextPartitionID =
        sendList.removeInt(random
          .nextInt(sendList.size()));
      P partition =
        table.getPartition(nextPartitionID);
      // Get the first worker ID in the dest list
      IntArrayList destWorkerIDs =
        allToAllStage1.getPartitionToWorkerMap()
          .get(nextPartitionID);
      PartitionUtils.multicastPartition(
        contextName, selfID, operationName,
        partition, destWorkerIDs, workers,
        resourcePool, true);
      // The first one in the list has been sent
      // to
      // for (int i = 1; i < destWorkerIDs.size();
      // i++) {
      // LOG.info("Send partition "
      // + nextPartitionID + " to "
      // + destWorkerIDs.get(i));
      // }
    }
    // --------------------------------------------------
    // Receive all the partitions
    int numRecvPartitions =
      allToAllStage2.getWorkerPartitionCountMap()
        .get(selfID);
    LOG.info("Total receive: "
      + numRecvPartitions);
    return PartitionUtils.receivePartitionsAlt(
      contextName, operationName, table,
      numRecvPartitions, rmPartitionIDs, dataMap,
      resourcePool);
  }
}
