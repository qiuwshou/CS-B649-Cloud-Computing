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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.iu.harp.array.ArrPartition;
import edu.iu.harp.array.ArrTable;
import edu.iu.harp.array.DoubleArrPlus;
import edu.iu.harp.client.DataSender;
import edu.iu.harp.comm.Communication;
import edu.iu.harp.io.ConnectionPool;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.DataType;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.io.IOUtils;
import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.PartitionUtils;
import edu.iu.harp.partition.Table;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.server.reuse.Server;
import edu.iu.harp.trans.DoubleArray;
import edu.iu.harp.worker.Workers;

public class ReduceCollective {

  private static final Logger LOG = Logger
    .getLogger(ReduceCollective.class);

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
        new ArrPartition<DoubleArray>(i,
          doubleArray);
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
    // Allreduce
    try {
      reduce(contextName, "allreduce", table,
        workers.getMasterID(), dataMap, workers,
        resourcePool);
    } catch (Exception e) {
      LOG.error("Fail to reduce", e);
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

  /**
   * Table could be modified during allgather
   * 
   * @param contextName
   * @param operationName
   * @param table
   * @param dataMap
   * @param workers
   * @param resourcePool
   * @return
   * @throws Exception
   */
  public static <P extends Partition> boolean
    reduce(final String contextName,
      final String operationName,
      final Table<P> table,
      final int reduceWorkerID,
      final DataMap dataMap,
      final Workers workers,
      final ResourcePool resourcePool) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    // Initially each worker only owns several
    // partitions (could be 0).
    int selfID = workers.getSelfID();
    int left = workers.getMinID();
    int right = workers.getMaxID();
    int middle = workers.getMiddleID();
    int half = middle - left + 1;
    int range = right - left + 1;
    int destID = 0;
    int sourceID = 0;
    // Check if this worker needs to send data
    // or receive data
    boolean isSend = false;
    boolean isRecv = false;
    boolean isFailed = false;
    Map<Integer, Data> cachedDataMap =
      new HashMap<>();
    while (left < right) {
      if (selfID <= middle
        && reduceWorkerID > middle) {
        destID = selfID + half;
        isSend = true;
        isRecv = false;
        // If the range is odd, middle's destID
        // will be out of range.
        if (destID > right) {
          destID = middle + 1;
        }
      } else if (selfID <= middle
        && reduceWorkerID <= middle) {
        sourceID = selfID + half;
        isSend = false;
        if (sourceID <= right) {
          isRecv = true;
        } else {
          isRecv = false;
        }
      } else if (selfID > middle
        && reduceWorkerID <= middle) {
        destID = selfID - half;
        isSend = true;
        isRecv = false;
      } else if (selfID > middle
        && reduceWorkerID > middle) {
        sourceID = selfID - half;
        isSend = false;
        isRecv = true;
      }
      // LOG.info("left " + left + ", right "
      // + right + ", middle " + middle
      // + ", half " + half + ", range " + range
      // + ", selfID " + selfID + ", destID "
      // + destID + ", sourceID " + sourceID
      // + ", isSend " + isSend + ", isRecv "
      // + isSend);
      if (isSend) {
        LinkedList<Partition> ownedPartitions =
          new LinkedList<>(table.getPartitions());
        int numOwnedPartitions =
          table.getNumPartitions();
        // Send owned partitions
        Data data =
          new Data(DataType.PARTITION_LIST,
            contextName, selfID, operationName,
            numOwnedPartitions, ownedPartitions);
        DataSender sender =
          new DataSender(data, destID, workers,
            resourcePool, Constants.SEND_DECODE);
        sender.execute();
        // Release
        data.releaseHeadArray(resourcePool);
        data.releaseEncodedBody(resourcePool);
        data = null;
        // Remove and release all the partitions
        // in the table
        PartitionUtils.releaseTable(resourcePool,
          table);
        // Once partitions are sent, it is done.
        break;
      } else if (isRecv) {
        Data data =
          cachedDataMap.remove(sourceID);
        // Wait data
        if (data == null) {
          while (true) {
            data =
              IOUtils.waitAndGet(dataMap,
                contextName, operationName);
            if (data == null) {
              isFailed = true;
              break;
            } else if (data.getWorkerID() != sourceID) {
              cachedDataMap.put(
                data.getWorkerID(), data);
            } else {
              break;
            }
          }
        }
        // Add partitions to the table, note that
        // the data has been decoded
        if (data != null) {
          List<Partition> recvPartitions =
            (List<Partition>) data.getBody();
          for (Partition partition : recvPartitions) {
            PartitionUtils.addPartitionToTable(
              partition, table, resourcePool);
          }
        }
        // If range is odd, midID + 1 receive
        // additional data from midID
        if (range % 2 == 1
          && selfID == (middle + 1) && !isFailed) {
          // LOG.info("Get extra data from middle: "
          // + middle);
          data = cachedDataMap.remove(middle);
          // Wait data
          if (data == null) {
            while (true) {
              data =
                IOUtils.waitAndGet(dataMap,
                  contextName, operationName);
              if (data == null) {
                isFailed = true;
                break;
              } else if (data.getWorkerID() != middle) {
                cachedDataMap.put(
                  data.getWorkerID(), data);
              } else {
                break;
              }
            }
          }
          // Add partitions to the table, note
          // that the data has been decoded
          if (data != null) {
            List<Partition> recvPartitions =
              (List<Partition>) data.getBody();
            for (Partition partition : recvPartitions) {
              PartitionUtils.addPartitionToTable(
                partition, table, resourcePool);
            }
          }
        }
        if (isFailed) {
          for (Entry<Integer, Data> entry : cachedDataMap
            .entrySet()) {
            entry.getValue()
              .release(resourcePool);
          }
          cachedDataMap.clear();
          cachedDataMap = null;
          // Release the partitions in the current
          // table
          PartitionUtils.releaseTable(
            resourcePool, table);
          return false;
        }
      }
      if (selfID <= middle) {
        right = middle;
      } else {
        left = middle + 1;
      }
      middle = (left + right) / 2;
      half = middle - left + 1;
      range = right - left + 1;
    }
    return true;
  }
}
