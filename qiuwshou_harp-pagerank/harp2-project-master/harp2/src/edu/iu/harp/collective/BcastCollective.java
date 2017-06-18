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
import edu.iu.harp.client.DataChainBcastSender;
import edu.iu.harp.client.DataMSTBcastSender;
import edu.iu.harp.client.Sender;
import edu.iu.harp.comm.Communication;
import edu.iu.harp.io.ConnectionPool;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.DataType;
import edu.iu.harp.io.EventQueue;
import edu.iu.harp.io.IOUtils;
import edu.iu.harp.message.Bcast;
import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.PartitionType;
import edu.iu.harp.partition.PartitionUtils;
import edu.iu.harp.partition.Table;
import edu.iu.harp.partition.TableType;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.server.reuse.Server;
import edu.iu.harp.trans.Array;
import edu.iu.harp.trans.ByteArray;
import edu.iu.harp.trans.DoubleArray;
import edu.iu.harp.trans.IntMatrix;
import edu.iu.harp.worker.Workers;

public class BcastCollective {
  /** Class logger */
  protected static final Logger LOG = Logger
    .getLogger(BcastCollective.class);

  public static void main(String args[])
    throws Exception {
    String driverHost = args[0];
    int driverPort = Integer.parseInt(args[1]);
    int workerID = Integer.parseInt(args[2]);
    long jobID = Long.parseLong(args[3]);
    int numBytes = Integer.parseInt(args[4]);
    int numLoops = Integer.parseInt(args[5]);
    // Initialize log
    Driver.initLogger(workerID);
    LOG.info("args[] " + driverHost + " "
      + driverPort + " " + workerID + " " + jobID
      + " " + numBytes + " " + numLoops);
    // ------------------------------------------
    // Worker initialize
    EventQueue eventQueue = new EventQueue();
    DataMap dataMap = new DataMap();
    Workers workers = new Workers(workerID);
    ResourcePool resourcePool =
      new ResourcePool();
    String host = workers.getSelfInfo().getNode();
    int port = workers.getSelfInfo().getPort();
    Server server =
      new Server(host, port,
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
    // ----------------------------------------------
    if (isSuccess) {
      try {
        runChainBcast(contextName,
          workers.getSelfID(), numBytes,
          numLoops, dataMap, workers,
          resourcePool);
      } catch (Exception e) {
        LOG.error("Fail to broadcast.", e);
      }
    }
    Driver.reportToDriver(contextName,
      "report-to-driver", workers.getSelfID(),
      driverHost, driverPort, resourcePool);
    ConnectionPool.closeAllConncetions();
    server.stop();
    System.exit(0);
  }

  public static boolean runChainBcast(
    String contextName, int workerID,
    int numBytes, int numLoops, DataMap dataMap,
    Workers workers, ResourcePool resourcePool) {
    boolean isSuccess =
      runByteArrayChainBcast(contextName,
        workerID, numBytes, numLoops, dataMap,
        workers, resourcePool);
    if (!isSuccess) {
      return false;
    }
    isSuccess =
      runDoubleArrayTableBcast(contextName,
        workerID, numBytes, numLoops, dataMap,
        workers, resourcePool);
    if (!isSuccess) {
      return false;
    }
    isSuccess =
      runIntMatrixBcast(contextName, workerID,
        numBytes, numLoops, dataMap, workers,
        resourcePool);
    if (!isSuccess) {
      return false;
    }
    return true;
  }

  private static ByteArray generateByteArray(
    int numBytes, ResourcePool pool) {
    long a = System.currentTimeMillis();
    byte[] bytes = pool.getBytes(numBytes);
    bytes[0] = (byte) (Math.random() * 255);
    bytes[numBytes - 1] =
      (byte) (Math.random() * 255);
    LOG.info("Generate ByteArray: First byte: "
      + bytes[0] + ", Last byte: "
      + bytes[numBytes - 1]);
    ByteArray byteArray =
      new ByteArray(bytes, 0, numBytes);
    LOG.info("Byte array generation time: "
      + (System.currentTimeMillis() - a));
    return byteArray;
  }

  public static boolean runByteArrayChainBcast(
    String contextName, int workerID,
    int numBytes, int numLoops, DataMap dataMap,
    Workers workers, ResourcePool resourcePool) {
    // Byte array bcast
    ByteArray byteArray = null;
    if (workers.isMaster()) {
      byteArray =
        generateByteArray(numBytes, resourcePool);
    }
    LinkedList<ByteArray> recvObjs = null;
    if (!workers.isMaster()) {
      recvObjs = new LinkedList<>();
    }
    boolean isSuccess = false;
    for (int i = 0; i < numLoops; i++) {
      long start = System.currentTimeMillis();
      isSuccess =
        Communication.chainBcastAndRecv(
          contextName, workers.getMasterID(),
          "byte-array-bcast",
          Constants.UNKNOWN_PARTITION_ID,
          byteArray, true, false, recvObjs,
          workers, resourcePool, dataMap, true);
      long end = System.currentTimeMillis();
      LOG.info("Loop " + i
        + " byte array bcast time: "
        + (end - start));
      // Release recv data
      if (!workers.isMaster()) {
        while (!recvObjs.isEmpty()) {
          ByteArray byteArr =
            recvObjs.removeFirst();
          LOG
            .info("Receive ByteArray: First byte: "
              + byteArr.getArray()[byteArr
                .getStart()]
              + ", Last byte: "
              + byteArr.getArray()[byteArr
                .getStart()
                + byteArr.getSize()
                - 1]);
          resourcePool.releaseBytes(byteArr
            .getArray());
          byteArr = null;
        }
      }
      if (!isSuccess) {
        break;
      }
    }
    // Release send data
    if (workers.isMaster()) {
      resourcePool.releaseBytes(byteArray
        .getArray());
      byteArray = null;
    }
    return isSuccess;
  }

  public static DoubleArray generateDoubleArray(
    int numBytes, ResourcePool pool) {
    long start = System.currentTimeMillis();
    int size = (int) (numBytes / (double) 8);
    double[] doubles = pool.getDoubles(size);
    doubles[0] = Math.random() * 255;
    doubles[size - 1] = Math.random() * 1000;
    LOG
      .info("Generate Double Array: First double: "
        + doubles[0]
        + ", last double: "
        + doubles[size - 1]);
    DoubleArray doubleArray =
      new DoubleArray(doubles, 0, size);
    long end = System.currentTimeMillis();
    LOG
      .info("Double array data generation time: "
        + (end - start));
    return doubleArray;
  }

  public static boolean runDoubleArrayTableBcast(
    String contextName, int workerID,
    int numBytes, int numLoops, DataMap dataMap,
    Workers workers, ResourcePool resourcePool) {
    // Double table bcast
    ArrTable<DoubleArray> arrTable =
      new ArrTable<>(new DoubleArrPlus());
    if (workers.isMaster()) {
      // Generate 8 partitions
      for (int i = 0; i < 8; i++) {
        DoubleArray doubleArray =
          generateDoubleArray(numBytes / 8,
            resourcePool);
        arrTable
          .addPartition(new ArrPartition<DoubleArray>(
            i, doubleArray));
      }
    }
    boolean isSuccess = false;
    for (int i = 0; i < numLoops; i++) {
      long start = System.currentTimeMillis();
      isSuccess =
        broadcast(contextName,
          "chain-array-table-bcast-" + i,
          arrTable, workers.getMasterID(), false,
          dataMap, workers, resourcePool);
      long end = System.currentTimeMillis();
      LOG
        .info("Total array table chain bcast time: "
          + (end - start));
      // Release
      if (!workers.isMaster()) {
        for (ArrPartition<DoubleArray> partition : arrTable
          .getPartitions()) {
          DoubleArray doubleArray =
            partition.getArray();
          LOG
            .info("Receive Double Array: First double: "
              + doubleArray.getArray()[doubleArray
                .getStart()]
              + ", last double: "
              + doubleArray.getArray()[doubleArray
                .getStart()
                + doubleArray.getSize() - 1]);
          resourcePool.releaseDoubles(doubleArray
            .getArray());
        }
        arrTable =
          new ArrTable<DoubleArray>(
            new DoubleArrPlus());
      }
      if (!isSuccess) {
        break;
      }
    }
    for (int i = 0; i < numLoops; i++) {
      long start = System.currentTimeMillis();
      isSuccess =
        broadcast(contextName,
          "mst-array-table-bcast-" + i, arrTable,
          workers.getMasterID(), true, dataMap,
          workers, resourcePool);
      long end = System.currentTimeMillis();
      LOG
        .info("Total array table mst bcast time: "
          + (end - start));
      // Release
      if (!workers.isMaster()) {
        for (ArrPartition<DoubleArray> partition : arrTable
          .getPartitions()) {
          DoubleArray doubleArray =
            partition.getArray();
          LOG
            .info("Receive Double Array: First double: "
              + doubleArray.getArray()[doubleArray
                .getStart()]
              + ", last double: "
              + doubleArray.getArray()[doubleArray
                .getStart()
                + doubleArray.getSize() - 1]);
          resourcePool.releaseDoubles(doubleArray
            .getArray());
        }
        arrTable =
          new ArrTable<DoubleArray>(
            new DoubleArrPlus());
      }
      if (!isSuccess) {
        break;
      }
    }
    // Release array table on Master
    if (workers.isMaster()) {
      for (ArrPartition<DoubleArray> partition : arrTable
        .getPartitions()) {
        resourcePool.releaseDoubles(partition
          .getArray().getArray());
      }
      arrTable =
        new ArrTable<>(new DoubleArrPlus());
    }
    return isSuccess;
  }

  private static IntMatrix generateIntMatrix(
    int numBytes, ResourcePool pool) {
    long start = System.currentTimeMillis();
    int matrixLen =
      (int) Math.sqrt(numBytes / (double) 4);
    IntMatrix matrix =
      pool.getWritableObject(IntMatrix.class);
    int[][] matrixBody =
      new int[matrixLen][matrixLen];
    matrixBody[matrixLen - 1][matrixLen - 1] = 1;
    matrix.updateMatrix(matrixBody, matrixLen,
      matrixLen);
    LOG.info("Int matrix generation time: "
      + (System.currentTimeMillis() - start));
    return matrix;
  }

  public static boolean runIntMatrixBcast(
    String contextName, int workerID,
    int numBytes, int numLoops, DataMap dataMap,
    Workers workers, ResourcePool resourcePool) {
    // Int matrix bcast
    IntMatrix intMatrix = null;
    if (workers.isMaster()) {
      intMatrix =
        generateIntMatrix(numBytes, resourcePool);
    }
    LinkedList<IntMatrix> recvObjs = null;
    if (!workers.isMaster()) {
      recvObjs = new LinkedList<>();
    }
    boolean isSuccess = false;
    long start = System.currentTimeMillis();
    isSuccess =
      Communication.chainBcastAndRecv(
        contextName, workers.getMasterID(),
        "int-matrix-bacast",
        Constants.UNKNOWN_PARTITION_ID,
        intMatrix, true, false, recvObjs,
        workers, resourcePool, dataMap, false);
    long end = System.currentTimeMillis();
    LOG.info("int matrix bcast time: "
      + (end - start));
    // Release recv data
    if (!workers.isMaster()) {
      while (!recvObjs.isEmpty()) {
        IntMatrix intMat = recvObjs.removeFirst();
        int row = intMat.getRow();
        int col = intMat.getCol();
        int[][] matrixBody =
          intMat.getMatrixBody();
        LOG.info("Last element: "
          + matrixBody[row - 1][col - 1]);
        resourcePool
          .releaseWritableObject(intMat);
      }
    }
    // Release send data
    if (workers.isMaster()) {
      resourcePool
        .releaseWritableObject(intMatrix);
      intMatrix = null;
    }
    return isSuccess;
  }

  public static
    <P extends Partition, TBL extends Table<P>>
    boolean broadcast(String contextName,
      String operationName, TBL table,
      int bcastWorkerID, boolean useMSTBcast,
      DataMap dataMap, Workers workers,
      ResourcePool resourcePool) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    if (workers.getSelfID() == bcastWorkerID) {
      int numOwnedPartitions =
        table.getNumPartitions();
      LinkedList<Partition> ownedPartitions =
        new LinkedList<>(table.getPartitions());
      Data data =
        new Data(DataType.PARTITION_LIST,
          contextName, bcastWorkerID,
          operationName, numOwnedPartitions,
          ownedPartitions);
      Sender sender = null;
      if (useMSTBcast) {
        sender =
          new DataMSTBcastSender(data, workers,
            resourcePool,
            Constants.MST_BCAST_DECODE);
      } else {
        sender =
          new DataChainBcastSender(data, workers,
            resourcePool,
            Constants.CHAIN_BCAST_DECODE);
      }
      boolean isSuccess = sender.execute();
      data.releaseHeadArray(resourcePool);
      data.releaseEncodedBody(resourcePool);
      data = null;
      // Wait for the ACK object
      // data =
      // IOUtils.waitAndGet(dataMap, contextName,
      // operationName);
      // if (data != null) {
      // data.release(resourcePool);
      // data = null;
      // }
      return isSuccess;
    } else {
      // Wait for data
      Data data =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      if (data != null) {
        List<Partition> partitions =
          (List<Partition>) data.getBody();
        PartitionType partitionType =
          PartitionUtils
            .getPartitionType(partitions.get(0));
        TableType tableType =
          PartitionUtils.getTableType(table);
        for (Partition partition : partitions) {
          PartitionUtils.addPartitionToTable(
            partition, partitionType, table,
            tableType, resourcePool);
        }
        data = null;
        // Send ACK
        // if (workers.getNextID() ==
        // bcastWorkerID) {
        // Ack ackObj =
        // resourcePool
        // .getWritableObject(Ack.class);
        // Data ackData =
        // new Data(DataType.STRUCT_OBJECT,
        // contextName, workers.getSelfID(),
        // operationName, ackObj);
        // Sender sender =
        // new DataSender(ackData,
        // bcastWorkerID, workers,
        // resourcePool, Constants.SEND_DECODE);
        // sender.execute();
        // ackData.release(resourcePool);
        // ackData = null;
        // }
        return true;
      } else {
        return false;
      }
    }
  }

  public static
    <T, A extends Array<T>, P extends Partition, TBL extends Table<P>>
    boolean broadcastLarge(String contextName,
      String operationName, Table<P> table,
      int bcastWorkerID, DataMap dataMap,
      Workers workers, ResourcePool resourcePool) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    int partitionCount =
      bcastParitionCount(contextName,
        operationName + ".bcast", table,
        bcastWorkerID, dataMap, workers,
        resourcePool);
    if (partitionCount > 0) {
      // If not error
      return broadcastLarge(contextName,
        operationName, table, partitionCount,
        bcastWorkerID, dataMap, workers,
        resourcePool);
    } else {
      return false;
    }
  }

  private static
    <P extends Partition, TBL extends Table<P>>
    int bcastParitionCount(String contextName,
      String operationName, TBL table,
      int bcastWorkerID, DataMap dataMap,
      Workers workers, ResourcePool resourcePool) {
    int selfID = workers.getSelfID();
    Bcast bcastMsg = null;
    List<Bcast> recvBcastMsg = null;
    if (selfID == bcastWorkerID) {
      bcastMsg =
        resourcePool
          .getWritableObject(Bcast.class);
      bcastMsg.setNumPartitions(PartitionUtils
        .getPartitionCountInSending(table,
          resourcePool));
    } else {
      recvBcastMsg = new LinkedList<>();
    }
    boolean isSuccess =
      Communication.mstBcastAndRecv(contextName,
        bcastWorkerID, operationName,
        Constants.UNKNOWN_PARTITION_ID, bcastMsg,
        true, false, recvBcastMsg, workers,
        resourcePool, dataMap);
    dataMap.cleanOperationData(contextName,
      operationName, resourcePool);
    if (selfID == bcastWorkerID) {
      if (!isSuccess) {
        resourcePool
          .releaseWritableObject(bcastMsg);
        // Error code
        return -1;
      }
    } else {
      if (isSuccess) {
        bcastMsg = recvBcastMsg.get(0);
        recvBcastMsg = null;
      } else {
        // Error code
        return -1;
      }
    }
    int numPartitions =
      bcastMsg.getNumPartitions();
    resourcePool.releaseWritableObject(bcastMsg);
    LOG
      .info("Total partitions to receive in bcast: "
        + numPartitions);
    return numPartitions;
  }

  public static
    <T, A extends Array<T>, P extends Partition, TBL extends Table<P>>
    boolean broadcastLarge(String contextName,
      String operationName, TBL table,
      int partitionCount, int bcastWorkerID,
      DataMap dataMap, Workers workers,
      ResourcePool resourcePool) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    if (workers.getSelfID() == bcastWorkerID) {
      boolean isSuccess = false;
      for (P partition : table.getPartitions()) {
        isSuccess =
          PartitionUtils.chainBcastPartition(
            contextName, bcastWorkerID,
            operationName, partition, workers,
            resourcePool, true);
        if (!isSuccess) {
          break;
        }
      }
      return isSuccess;
    } else {
      return PartitionUtils.receivePartitions(
        contextName, operationName, table,
        partitionCount,
        new LinkedList<Integer>(), dataMap,
        resourcePool);
    }
  }
}
