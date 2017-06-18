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

package edu.iu.harp.comm;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.iu.harp.client.DataChainBcastSender;
import edu.iu.harp.client.DataMSTBcastSender;
import edu.iu.harp.client.DataSender;
import edu.iu.harp.client.Sender;
import edu.iu.harp.io.Constants;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.DataMap;
import edu.iu.harp.io.DataStatus;
import edu.iu.harp.io.DataType;
import edu.iu.harp.io.DataUtils;
import edu.iu.harp.io.IOUtils;
import edu.iu.harp.message.Ack;
import edu.iu.harp.message.Barrier;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.ByteArray;
import edu.iu.harp.trans.DoubleArray;
import edu.iu.harp.trans.IntArray;
import edu.iu.harp.trans.StructObject;
import edu.iu.harp.trans.Transferable;
import edu.iu.harp.worker.WorkerInfo;
import edu.iu.harp.worker.Workers;

public class Communication {
  /** Class logger */
  protected static final Logger LOG = Logger
    .getLogger(Communication.class);

  /**
   * In barrier, each worker send a message to
   * master. If the master gets all the messages,
   * sends true to all workers to leave the
   * barrier. Else it sends false to all the
   * workers.
   * 
   * @param workers
   * @param workerData
   * @param resourcePool
   * @return
   */
  public static boolean barrier(
    String contextName, String operationName,
    DataMap dataMap, Workers workers,
    ResourcePool resourcePool) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    // Send barrier and wait for replies
    if (workers.isMaster()) {
      boolean isBarrierSuccess = true;
      int numWorkers = workers.getNumWorkers();
      // Collect replies from other workers
      Data recvData = null;
      int i = 1;
      while (i < numWorkers) {
        recvData =
          IOUtils.waitAndGet(dataMap,
            contextName, operationName);
        if (recvData != null) {
          LOG.info("Barrier from Worker "
            + recvData.getWorkerID());
          recvData.release(resourcePool);
          recvData = null;
          i++;
        } else {
          LOG.info("Workers may fail.");
          isBarrierSuccess = false;
          break;
        }
      }
      // Create the barrier data
      Barrier barrier =
        resourcePool
          .getWritableObject(Barrier.class);
      barrier.setStatus(isBarrierSuccess);
      // Send the barrier info to every worker
      Data sendData =
        new Data(DataType.STRUCT_OBJECT,
          contextName, workers.getSelfID(),
          operationName, barrier);
      for (WorkerInfo worker : workers
        .getWorkerInfoList()) {
        if (worker.getID() != workers
          .getMasterID()) {
          Sender sender =
            new DataSender(sendData,
              worker.getID(), workers,
              resourcePool, Constants.SEND_DECODE);
          boolean isSuccess = false;
          int retryCount = 0;
          do {
            isSuccess = sender.execute();
            if (!isSuccess) {
              retryCount++;
              try {
                Thread
                  .sleep(Constants.SHORT_SLEEP);
              } catch (InterruptedException e) {
              }
            }
          } while (!isSuccess
            && retryCount < Constants.SMALL_RETRY_COUNT);
          if (!isSuccess) {
            isBarrierSuccess = false;
          }
        }
      }
      // Release all the resource used
      sendData.release(resourcePool);
      sendData = null;
      barrier = null;
      return isBarrierSuccess;
    } else {
      // From slave workers
      boolean isBarrierSuccess = false;
      // Create the barrier object
      Barrier barrier =
        resourcePool
          .getWritableObject(Barrier.class);
      barrier.setStatus(true);
      // Create the barrier data
      Data sendData =
        new Data(DataType.STRUCT_OBJECT,
          contextName, workers.getSelfID(),
          operationName, barrier);
      Sender sender =
        new DataSender(sendData,
          workers.getMasterID(), workers,
          resourcePool, Constants.SEND_DECODE);
      // Send to master
      // TODO think about adding fault tolerance
      boolean isSuccess = false;
      int retryCount = 0;
      do {
        LOG.info("Start sending barrier.");
        isSuccess = sender.execute();
        LOG.info("Send barrier: " + isSuccess);
        if (!isSuccess) {
          retryCount++;
          try {
            Thread.sleep(Constants.SHORT_SLEEP);
          } catch (InterruptedException e) {
          }
        }
      } while (!isSuccess
        && retryCount < Constants.LARGE_RETRY_COUNT);
      // Release all the resource used
      sendData.release(resourcePool);
      sendData = null;
      barrier = null;
      if (!isSuccess) {
        return false;
      }
      // Wait for the reply from master
      Data recvData =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      if (recvData != null) {
        LOG.info("Barrier is received.");
        barrier = (Barrier) recvData.getBody();
        if (barrier.getStatus()) {
          isBarrierSuccess = true;
        }
        recvData.release(resourcePool);
        recvData = null;
      }
      return isBarrierSuccess;
    }
  }

  public static <S extends StructObject> boolean
    structObjGather(String contextName,
      String operationName, S sendObj,
      List<S> recvObj, int gatherWorkerID,
      DataMap dataMap, Workers workers,
      ResourcePool resourcePool) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    if (workers.getSelfID() == gatherWorkerID) {
      int numWorkers = workers.getNumWorkers();
      int count = 1;
      boolean isFailed = false;
      Data data = null;
      while (count < numWorkers) {
        data =
          IOUtils.waitAndGet(dataMap,
            contextName, operationName);
        if (data != null) {
          recvObj.add((S) data.getBody());
          data = null;
          count++;
        } else {
          isFailed = true;
          data = null;
          break;
        }
      }
      if (isFailed) {
        // We need to clean the received data
        // The data is decoded, we only need to
        // release object
        for (S obj : recvObj) {
          resourcePool.releaseWritableObject(obj);
        }
        recvObj.clear();
        return false;
      }
      return true;
    } else {
      Data data =
        new Data(DataType.STRUCT_OBJECT,
          contextName, workers.getSelfID(),
          operationName, sendObj);
      Sender sender =
        new DataSender(data, gatherWorkerID,
          workers, resourcePool,
          Constants.SEND_DECODE);
      boolean success = sender.execute();
      data.releaseHeadArray(resourcePool);
      data.releaseEncodedBody(resourcePool);
      data = null;
      return success;
    }
  }

  public static <T extends Transferable> boolean
    allbcast(String contextName,
      String operationName, T sendObj,
      List<T> recvObjs, int selfID,
      int numWorkers, DataMap dataMap,
      Workers workers, ResourcePool resourcePool) {
    if (numWorkers > 1) {
      boolean isSuccess =
        Communication.bcast(contextName, selfID,
          operationName,
          Constants.UNKNOWN_PARTITION_ID,
          sendObj, true, false, workers,
          resourcePool, true);
      if (isSuccess) {
        boolean isFailed = false;
        for (int i = 1; i < numWorkers; i++) {
          Data data =
            IOUtils.waitAndGet(dataMap,
              contextName, operationName);
          if (data != null) {
            recvObjs.add((T) data.getBody());
            data = null;
          } else {
            isFailed = true;
            break;
          }
        }
        if (isFailed) {
          for (T recvObj : recvObjs) {
            DataUtils.releaseTrans(resourcePool,
              recvObj);
          }
          recvObjs.clear();
        } else {
          recvObjs.add(sendObj);
        }
        return !isFailed;
      } else {
        return false;
      }
    } else {
      recvObjs.add(sendObj);
      return true;
    }
  }

  public static <T extends Transferable> boolean
    allgather(final String contextName,
      final String operationName,
      final byte dataType, T sendObj,
      List<T> recvObjs, final int selfID,
      final int numWorkers, DataMap dataMap,
      Workers workers, ResourcePool resourcePool) {
    if (numWorkers > 1) {
      final int nextID = workers.getNextID();
      boolean isSuccess =
        Communication.send(contextName, selfID,
          operationName,
          Constants.UNKNOWN_PARTITION_ID,
          dataType, sendObj, true, false, nextID,
          workers, resourcePool, false);
      if (isSuccess) {
        boolean isFailed = false;
        for (int i = 1; i < numWorkers; i++) {
          Data data =
            IOUtils.waitAndGet(dataMap,
              contextName, operationName);
          if (data != null) {
            if (data.getWorkerID() != nextID) {
              DataSender sender =
                new DataSender(data, nextID,
                  workers, resourcePool,
                  Constants.SEND);
              sender.execute();
            }
            try {
              data.decodeBodyArray(resourcePool);
              recvObjs.add((T) data.getBody());
              data.releaseHeadArray(resourcePool);
              data.releaseBodyArray(resourcePool);
            } catch (Exception e) {
              LOG.error("Fail to decode.", e);
            }
          } else {
            isFailed = true;
            break;
          }
        }
        if (isFailed) {
          DataUtils.releaseTrans(resourcePool,
            recvObjs, dataType);
          recvObjs.clear();
        } else {
          recvObjs.add(sendObj);
        }
        return !isFailed;
      } else {
        return false;
      }
    } else {
      recvObjs.add(sendObj);
      return true;
    }
  }

  public static <T extends Transferable> boolean
    send(String contextName, int sourceWorkerID,
      String operationName, int partitionID,
      T sendObj, boolean isOperation,
      boolean isPartition, int destWorkerID,
      Workers workers, ResourcePool resourcePool,
      boolean decode) {
    Data data =
      createData(contextName, sourceWorkerID,
        operationName, partitionID, sendObj,
        isOperation, isPartition);
    if (data != null) {
      Sender sender = null;
      if (decode) {
        sender =
          new DataSender(data, destWorkerID,
            workers, resourcePool,
            Constants.SEND_DECODE);
      } else {
        sender =
          new DataSender(data, destWorkerID,
            workers, resourcePool, Constants.SEND);
      }
      boolean isSuccess = sender.execute();
      if (!isSuccess) {
        LOG.info("Fail to send.");
      }
      data.releaseHeadArray(resourcePool);
      data.releaseEncodedBody(resourcePool);
      data = null;
      return isSuccess;
    } else {
      return false;
    }
  }
  
  public static <T extends Transferable> boolean
    send(String contextName, int sourceWorkerID,
      String operationName, int partitionID,
      byte dataType, T sendObj,
      boolean isOperation, boolean isPartition,
      int destWorkerID, Workers workers,
      ResourcePool resourcePool, boolean decode) {
    Data data =
      createData(contextName, sourceWorkerID,
        operationName, partitionID, dataType,
        sendObj, isOperation, isPartition);
    if (data != null) {
      Sender sender = null;
      if (decode) {
        sender =
          new DataSender(data, destWorkerID,
            workers, resourcePool,
            Constants.SEND_DECODE);
      } else {
        sender =
          new DataSender(data, destWorkerID,
            workers, resourcePool, Constants.SEND);
      }
      boolean isSuccess = sender.execute();
      if (!isSuccess) {
        LOG.info("Fail to send.");
      }
      data.releaseHeadArray(resourcePool);
      data.releaseEncodedBody(resourcePool);
      data = null;
      return isSuccess;
    } else {
      return false;
    }
  }

  public static <T extends Transferable> boolean
    multicast(String contextName,
      int sourceWorkerID, String operationName,
      int partitionID, T sendObj,
      boolean isOperation, boolean isPartition,
      List<Integer> destWorkerIDs,
      Workers workers, ResourcePool resourcePool,
      boolean decode) {
    Data data =
      createData(contextName, sourceWorkerID,
        operationName, partitionID, sendObj,
        isOperation, isPartition);
    if (data != null) {
      Sender sender = null;
      boolean isSuccess = false;
      boolean isAllSuccess = true;
      for (int destWorkerID : destWorkerIDs) {
        if (sourceWorkerID != destWorkerID) {
          if (decode) {
            sender =
              new DataSender(data, destWorkerID,
                workers, resourcePool,
                Constants.SEND_DECODE);
          } else {
            sender =
              new DataSender(data, destWorkerID,
                workers, resourcePool,
                Constants.SEND);
          }
          isSuccess = sender.execute();
          if (!isSuccess) {
            isAllSuccess = false;
            LOG
              .info("Fail to send in multicast.");
          }
        }
      }
      data.releaseHeadArray(resourcePool);
      data.releaseEncodedBody(resourcePool);
      data = null;
      return isAllSuccess;
    } else {
      return false;
    }
  }

  public static <T extends Transferable> boolean
    multicastOrCopy(String contextName,
      int sourceWorkerID, String operationName,
      int partitionID, T sendObj,
      boolean isOperation, boolean isPartition,
      List<Integer> destWorkerIDs,
      DataMap dataMap, Workers workers,
      ResourcePool resourcePool, boolean decode) {
    Data data =
      createData(contextName, sourceWorkerID,
        operationName, partitionID, sendObj,
        isOperation, isPartition);
    if (data != null) {
      Sender sender = null;
      boolean isSuccess = false;
      boolean isAllSuccess = true;
      boolean isSentToLocal = false;
      if (destWorkerIDs.size() == workers
        .getNumWorkers()) {
        if (decode) {
          sender =
            new DataMSTBcastSender(data, workers,
              resourcePool,
              Constants.MST_BCAST_DECODE);
        } else {
          sender =
            new DataMSTBcastSender(data, workers,
              resourcePool, Constants.MST_BCAST);
        }
        isSuccess = sender.execute();
        if (!isSuccess) {
          isAllSuccess = false;
          LOG.info("Fail to broadcast.");
        }
        isSentToLocal = true;
      } else {
        for (int destWorkerID : destWorkerIDs) {
          if (sourceWorkerID != destWorkerID) {
            if (decode) {
              sender =
                new DataSender(data,
                  destWorkerID, workers,
                  resourcePool,
                  Constants.SEND_DECODE);
            } else {
              sender =
                new DataSender(data,
                  destWorkerID, workers,
                  resourcePool, Constants.SEND);
            }
            isSuccess = sender.execute();
            if (!isSuccess) {
              isAllSuccess = false;
              LOG.info("Fail to multicast.");
              break;
            }
          } else {
            isSentToLocal = true;
          }
        }
      }
      if (!isSentToLocal) {
        data.releaseHeadArray(resourcePool);
        data.releaseEncodedBody(resourcePool);
        data = null;
        return isAllSuccess;
      } else {
        // LOG.info("isSentToLocal true");
        if (!isAllSuccess) {
          data.releaseHeadArray(resourcePool);
          data.releaseEncodedBody(resourcePool);
          data = null;
          return false;
        } else {
          // If not serialized in previous
          // sendings, encode when necessary
          data.encodeHead(resourcePool);
          data.encodeBody(resourcePool);
          Data newData =
            new Data(data.getHeadArray(),
              data.getBodyArray());
          data = null;
          // LOG
          // .info("Before decode: New data head status: "
          // + newData.getHeadStatus()
          // + ", New data body status:"
          // + newData.getBodyStatus());
          newData.decodeHeadArray(resourcePool);
          if (decode) {
            newData
              .releaseHeadArray(resourcePool);
            newData.decodeBodyArray(resourcePool);
            newData
              .releaseBodyArray(resourcePool);
          }
          // LOG
          // .info("After decode: New data head status: "
          // + newData.getHeadStatus()
          // + ", New data body status:"
          // + newData.getBodyStatus());
          // If encoded with stream
          // it will fail here
          if ((newData.getHeadStatus() == DataStatus.DECODED && newData
            .getBodyStatus() == DataStatus.DECODED)
            || (newData.getHeadStatus() == DataStatus.ENCODED_ARRAY_DECODED && newData
              .getBodyStatus() == DataStatus.ENCODED_ARRAY)
            && newData.isOperationData()) {
            // LOG.info("Put local data");
            dataMap.putData(newData);
            return true;
          } else {
            // If cannot be put to the data map
            // try to release
            newData
              .releaseHeadArray(resourcePool);
            newData
              .releaseEncodedBody(resourcePool);
            newData = null;
            return false;
          }
        }
      }
    } else {
      return false;
    }
  }

  public static <T extends Transferable> boolean
    bcast(String contextName, int bcastWorkerID,
      String operationName, int partitionID,
      T sendObj, boolean isOperation,
      boolean isPartition, Workers workers,
      ResourcePool resourcePool, boolean decode) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    if (workers.getSelfID() != bcastWorkerID) {
      return false;
    }
    Data data =
      createData(contextName, bcastWorkerID,
        operationName, partitionID, sendObj,
        isOperation, isPartition);
    if (data != null) {
      data.encodeHead(resourcePool);
      data.encodeBody(resourcePool);
      Sender sender = null;
      if (data.getBodyArray() != null) {
        if (data.getBodyArray().getSize() > Constants.MAX_MST_BCAST_BYTE_SIZE) {
          if (decode) {
            sender =
              new DataChainBcastSender(data,
                workers, resourcePool,
                Constants.CHAIN_BCAST_DECODE);
          } else {
            sender =
              new DataChainBcastSender(data,
                workers, resourcePool,
                Constants.CHAIN_BCAST);
          }
        } else {
          if (decode) {
            sender =
              new DataMSTBcastSender(data,
                workers, resourcePool,
                Constants.MST_BCAST_DECODE);
          } else {
            sender =
              new DataMSTBcastSender(data,
                workers, resourcePool,
                Constants.MST_BCAST);
          }
        }
      }
      boolean isSuccess = sender.execute();
      data.releaseHeadArray(resourcePool);
      data.releaseEncodedBody(resourcePool);
      data = null;
      return isSuccess;
    } else {
      return false;
    }
  }

  public static <T extends Transferable> boolean
    bcastAndRecv(String contextName,
      int bcastWorkerID, String operationName,
      int partitionID, T sendObj,
      boolean isOperation, boolean isPartition,
      List<T> recvObj, Workers workers,
      ResourcePool resourcePool, DataMap dataMap) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    if (workers.getSelfID() == bcastWorkerID) {
      return bcast(contextName, bcastWorkerID,
        operationName, partitionID, sendObj,
        isOperation, isPartition, workers,
        resourcePool, true);
    } else {
      // Wait for data
      Data data =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      if (data != null) {
        recvObj.add((T) data.getBody());
        data = null;
        return true;
      } else {
        return false;
      }
    }
  }

  public static <T extends Transferable> boolean
    chainBcast(String contextName,
      int bcastWorkerID, String operationName,
      int partitionID, T sendObj,
      boolean isOperation, boolean isPartition,
      Workers workers, ResourcePool resourcePool,
      boolean decode) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    if (workers.getSelfID() != bcastWorkerID) {
      return false;
    }
    Data data =
      createData(contextName, bcastWorkerID,
        operationName, partitionID, sendObj,
        isOperation, isPartition);
    if (data != null) {
      Sender sender = null;
      if (decode) {
        sender =
          new DataChainBcastSender(data, workers,
            resourcePool,
            Constants.CHAIN_BCAST_DECODE);
      } else {
        sender =
          new DataChainBcastSender(data, workers,
            resourcePool, Constants.CHAIN_BCAST);
      }
      boolean isSuccess = sender.execute();
      data.releaseHeadArray(resourcePool);
      data.releaseEncodedBody(resourcePool);
      data = null;
      return isSuccess;
    } else {
      return false;
    }
  }

  public static <T extends Transferable> boolean
    chainBcastAndRecv(String contextName,
      int bcastWorkerID, String operationName,
      int partitionID, T sendObj,
      boolean isOperation, boolean isPartition,
      List<T> recvObj, Workers workers,
      ResourcePool resourcePool, DataMap dataMap,
      boolean ack) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    if (workers.getSelfID() == bcastWorkerID) {
      boolean isSuccess =
        chainBcast(contextName, bcastWorkerID,
          operationName, partitionID, sendObj,
          isOperation, isPartition, workers,
          resourcePool, true);
      if (isSuccess && ack) {
        Data data =
          IOUtils.waitAndGet(dataMap,
            contextName, operationName);
        if (data != null) {
          data.release(resourcePool);
          data = null;
          return true;
        } else {
          LOG.error("No Chain Bcast ACK.");
          return false;
        }
      } else {
        return isSuccess;
      }
    } else {
      Data data =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      if (data != null) {
        recvObj.add((T) data.getBody());
        if (ack
          && workers.getNextID() == bcastWorkerID) {
          // Create the barrier object
          Ack ackObj =
            resourcePool
              .getWritableObject(Ack.class);
          // Create the barrier data
          Data ackData =
            new Data(DataType.STRUCT_OBJECT,
              contextName, workers.getSelfID(),
              operationName, ackObj);
          Sender sender =
            new DataSender(ackData,
              bcastWorkerID, workers,
              resourcePool, Constants.SEND_DECODE);
          boolean isSuccess = sender.execute();
          ackData.release(resourcePool);
          ackData = null;
          ackObj = null;
          return isSuccess;
        } else {
          return true;
        }
      } else {
        return false;
      }
    }
  }

  public static <T extends Transferable> boolean
    mstBcast(String contextName,
      int bcastWorkerID, String operationName,
      int partitionID, T sendObj,
      boolean isOperation, boolean isPartition,
      Workers workers, ResourcePool resourcePool,
      boolean decode) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    if (workers.getSelfID() != bcastWorkerID) {
      return false;
    }
    Data data =
      createData(contextName, bcastWorkerID,
        operationName, partitionID, sendObj,
        isOperation, isPartition);
    if (data != null) {
      Sender sender = null;
      if (decode) {
        sender =
          new DataMSTBcastSender(data, workers,
            resourcePool,
            Constants.MST_BCAST_DECODE);
      } else {
        new DataMSTBcastSender(data, workers,
          resourcePool, Constants.MST_BCAST);
      }
      boolean isSuccess = sender.execute();
      data.releaseHeadArray(resourcePool);
      data.releaseEncodedBody(resourcePool);
      data = null;
      return isSuccess;
    } else {
      return false;
    }
  }

  public static <T extends Transferable> boolean
    mstBcastAndRecv(String contextName,
      int bcastWorkerID, String operationName,
      int partitionID, T sendObj,
      boolean isOperation, boolean isPartition,
      List<T> recvObj, Workers workers,
      ResourcePool resourcePool, DataMap dataMap) {
    if (workers.isTheOnlyWorker()) {
      return true;
    }
    if (workers.getSelfID() == bcastWorkerID) {
      return mstBcast(contextName, bcastWorkerID,
        operationName, partitionID, sendObj,
        isOperation, isPartition, workers,
        resourcePool, true);
    } else {
      // Wait for data
      Data data =
        IOUtils.waitAndGet(dataMap, contextName,
          operationName);
      if (data != null) {
        recvObj.add((T) data.getBody());
        data = null;
        return true;
      } else {
        return false;
      }
    }
  }

  private static <T extends Transferable> Data
    createData(String contextName, int workerID,
      String operationName, int partitionID,
      T sendObj, boolean isOperation,
      boolean isPartition) {
    Data data = null;
    if (sendObj instanceof ByteArray) {
      if (isPartition) {
        data =
          new Data(DataType.BYTE_ARRAY,
            contextName, workerID, operationName,
            partitionID, sendObj);
      } else if (isOperation) {
        data =
          new Data(DataType.BYTE_ARRAY,
            contextName, workerID, operationName,
            sendObj);
      } else {
        data =
          new Data(DataType.BYTE_ARRAY,
            contextName, workerID, sendObj);
      }
    } else if (sendObj instanceof IntArray) {
      if (isPartition) {
        data =
          new Data(DataType.INT_ARRAY,
            contextName, workerID, operationName,
            partitionID, sendObj);
      } else if (isOperation) {
        data =
          new Data(DataType.INT_ARRAY,
            contextName, workerID, operationName,
            sendObj);
      } else {
        data =
          new Data(DataType.INT_ARRAY,
            contextName, workerID, sendObj);
      }
    } else if (sendObj instanceof DoubleArray) {
      if (isPartition) {
        data =
          new Data(DataType.DOUBLE_ARRAY,
            contextName, workerID, operationName,
            partitionID, sendObj);
      } else if (isOperation) {
        data =
          new Data(DataType.DOUBLE_ARRAY,
            contextName, workerID, operationName,
            sendObj);
      } else {
        data =
          new Data(DataType.DOUBLE_ARRAY,
            contextName, workerID, sendObj);
      }
    } else if (sendObj instanceof StructObject) {
      if (isPartition) {
        data =
          new Data(DataType.STRUCT_OBJECT,
            contextName, workerID, operationName,
            partitionID, sendObj);
      } else if (isOperation) {
        data =
          new Data(DataType.STRUCT_OBJECT,
            contextName, workerID, operationName,
            sendObj);
      } else {
        data =
          new Data(DataType.STRUCT_OBJECT,
            contextName, workerID, sendObj);
      }
    } else {
      LOG.error("Fail to get data type: "
        + sendObj.getClass().getName());
    }
    return data;
  }

  private static <T extends Transferable> Data
    createData(String contextName, int workerID,
      String operationName, int partitionID,
      byte type, T sendObj, boolean isOperation,
      boolean isPartition) {
    Data data = null;
    if (isPartition) {
      data =
        new Data(type, contextName, workerID,
          operationName, partitionID, sendObj);
    } else if (isOperation) {
      data =
        new Data(type, contextName, workerID,
          operationName, sendObj);
    } else {
      data =
        new Data(type, contextName, workerID,
          sendObj);
    }
    return data;
  }
}
