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

package edu.iu.harp.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.log4j.Logger;

import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.PartitionUtils;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.ByteArray;
import edu.iu.harp.trans.DoubleArray;
import edu.iu.harp.trans.IntArray;
import edu.iu.harp.trans.StructObject;
import edu.iu.harp.trans.Transferable;
import edu.iu.harp.trans.WritableObject;

/**
 * The fields in data must be consistent.
 * 
 * @author zhangbj
 *
 */
public class Data {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(Data.class);
  /** The type of body data, array or object */
  private byte bodyType =
    DataType.UNKNOWN_DATA_TYPE;
  /** This usually is the name of event handler */
  private String contextName = null;
  /** Source worker ID */
  private int workerID =
    Constants.UNKNOWN_WORKER_ID;
  /** Only required in collective communication */
  private String operationName = null;
  /** Only required in collective communication */
  private int partitionID =
    Constants.UNKNOWN_PARTITION_ID;
  /** Data object contained */
  private Object body = null;

  private ByteArray headArray = null;
  private ByteArray bodyArray = null;
  private ByteArrayOutputStream bodyStream = null;

  private DataStatus headStatus;
  private DataStatus bodyStatus;

  /** Construct a data */
  public Data(byte type, String conName, int wID,
    Object object) {
    bodyType = type;
    contextName = conName;
    workerID = wID;
    body = object;
    headStatus = DataStatus.DECODED;
    bodyStatus = DataStatus.DECODED;
    if (!isData() || body == null) {
      resetData();
      headStatus = DataStatus.DECODE_FAILED;
      bodyStatus = DataStatus.DECODE_FAILED;
    }
  }

  /** Construct an operation data */
  public Data(byte type, String conName, int wID,
    String opName, Object object) {
    bodyType = type;
    contextName = conName;
    workerID = wID;
    operationName = opName;
    body = object;
    headStatus = DataStatus.DECODED;
    bodyStatus = DataStatus.DECODED;
    if (!isOperationData() || body == null) {
      resetData();
      headStatus = DataStatus.DECODE_FAILED;
      bodyStatus = DataStatus.DECODE_FAILED;
    }
  }

  /** Construct a partition data in an operation */
  public Data(byte type, String conName, int wID,
    String opName, int parID, Object object) {
    bodyType = type;
    contextName = conName;
    workerID = wID;
    operationName = opName;
    partitionID = parID;
    body = object;
    headStatus = DataStatus.DECODED;
    bodyStatus = DataStatus.DECODED;
    if (!isPartitionData() || body == null) {
      resetData();
      headStatus = DataStatus.DECODE_FAILED;
      bodyStatus = DataStatus.DECODE_FAILED;
    }
  }

  public Data(ByteArray headArr, ByteArray bodyArr) {
    headArray = headArr;
    bodyArray = bodyArr;
    headStatus = DataStatus.ENCODED_ARRAY;
    bodyStatus = DataStatus.ENCODED_ARRAY;
    if (headArray == null
      || headArray.getArray() == null
      || bodyArray == null
      || bodyArray.getArray() == null) {
      headArray = null;
      headStatus = DataStatus.DECODE_FAILED;
      bodyArray = null;
      bodyStatus = DataStatus.DECODE_FAILED;
    }
  }

  /** Data > Operation Data > Partition Data */
  public boolean isData() {
    if (bodyType == DataType.UNKNOWN_DATA_TYPE
      || contextName == null
    // We don't detect worker ID now
    // || workerID == Constants.UNKNOWN_WORKER_ID
    ) {
      return false;
    }
    return true;
  }

  public boolean isOperationData() {
    if (operationName == null) {
      return false;
    } else {
      return isData();
    }
  }

  public boolean isPartitionData() {
    if (partitionID == Constants.UNKNOWN_PARTITION_ID) {
      return false;
    } else {
      return isOperationData();
    }
  }

  private void resetData() {
    bodyType = DataType.UNKNOWN_DATA_TYPE;
    contextName = null;
    workerID = Constants.UNKNOWN_WORKER_ID;
    operationName = null;
    partitionID = Constants.UNKNOWN_PARTITION_ID;
    body = null;
  }

  public byte getBodyType() {
    return bodyType;
  }

  public String getContextName() {
    return contextName;
  }

  public int getWorkerID() {
    return workerID;
  }

  public String getOperationName() {
    return operationName;
  }

  public int getPartitionID() {
    return partitionID;
  }

  public Object getBody() {
    return body;
  }

  public ByteArray getHeadArray() {
    return headArray;
  }

  public ByteArray getBodyArray() {
    return bodyArray;
  }

  public DataStatus getHeadStatus() {
    return headStatus;
  }

  public DataStatus getBodyStatus() {
    return bodyStatus;
  }

  public ByteArrayOutputStream getBodyStream() {
    return bodyStream;
  }

  public void releaseHeadArray(ResourcePool pool) {
    // Release the head array
    // If the array is removed without
    // encode/decode, make sure if the data status
    // is correct
    // Other states shouldn't have encoded head
    // array
    if (headStatus == DataStatus.ENCODED_ARRAY_DECODED) {
      pool.releaseBytes(headArray.getArray());
      headArray = null;
      headStatus = DataStatus.DECODED;
    } else if (headStatus == DataStatus.ENCODED_ARRAY) {
      pool.releaseBytes(headArray.getArray());
      headArray = null;
      headStatus = DataStatus.DECODE_FAILED;
    } else if (headStatus == DataStatus.ENCODED_ARRAY_DECODE_FAILED) {
      pool.releaseBytes(headArray.getArray());
      headArray = null;
      headStatus = DataStatus.DECODE_FAILED;
    }
  }

  public void releaseBodyArray(ResourcePool pool) {
    // Release the data array
    // If the array is removed without
    // encode/decode, make sure if the data status
    // is correct
    // Avoid releasing the real data
    // Other states shouldn't have encoded body
    // array
    if (bodyStatus == DataStatus.ENCODED_ARRAY_DECODED) {
      if (body instanceof ByteArray) {
        bodyArray = null;
      } else {
        pool.releaseBytes(bodyArray.getArray());
      }
      bodyStatus = DataStatus.DECODED;
    } else if (bodyStatus == DataStatus.ENCODED_ARRAY) {
      pool.releaseBytes(bodyArray.getArray());
      bodyArray = null;
      bodyStatus = DataStatus.DECODE_FAILED;
    } else if (bodyStatus == DataStatus.ENCODED_ARRAY_DECODE_FAILED) {
      pool.releaseBytes(bodyArray.getArray());
      bodyArray = null;
      bodyStatus = DataStatus.DECODE_FAILED;
    }
  }

  public void
    releaseBodyStream(ResourcePool pool) {
    // Release the data array
    // If the array is removed without
    // encode/decode, make sure if the data status
    // is correct
    // Other states should not have encoded body
    // stream
    if (bodyStatus == DataStatus.ENCODED_STREAM_DECODED) {
      pool.getByteArrayOutputStreamPool()
        .releaseByteArrayOutputStreamInUse(
          bodyStream);
      bodyStream = null;
      bodyStatus = DataStatus.DECODED;
    }
  }

  public void
    releaseEncodedBody(ResourcePool pool) {
    releaseBodyArray(pool);
    releaseBodyStream(pool);
  }

  public void release(ResourcePool pool) {
    releaseHeadArray(pool);
    releaseEncodedBody(pool);
    // In these two cases, we need to release body
    // object
    if (bodyStatus == DataStatus.DECODED
      || bodyStatus == DataStatus.ENCODE_FAILED_DECODED) {
      if (bodyType == DataType.BYTE_ARRAY
        || bodyType == DataType.INT_ARRAY
        || bodyType == DataType.DOUBLE_ARRAY
        || bodyType == DataType.STRUCT_OBJECT) {
        DataUtils.releaseTrans(pool,
          (Transferable) body);
      } else if (bodyType == DataType.TRANS_LIST) {
        List<Transferable> objs =
          (List<Transferable>) body;
        for (Transferable obj : objs) {
          DataUtils.releaseTrans(pool, obj);
        }
      } else if (bodyType == DataType.PARTITION_LIST) {
        List<Partition> partitions =
          (List<Partition>) body;
        for (Partition partition : partitions) {
          PartitionUtils.releasePartition(pool,
            partition);
        }
      } else {
        System.out
          .println("Unknown Body Type. Cannot release.");
      }
    }
    resetData();
    headStatus = DataStatus.DECODE_FAILED;
    bodyStatus = DataStatus.DECODE_FAILED;
  }

  public DataStatus decodeHeadArray(
    ResourcePool pool) {
    if (headStatus == DataStatus.ENCODED_ARRAY) {
      // Decode head array to fields
      // If head array is null, the status cannot
      // be encoded array
      Deserializer deserializer =
        new Deserializer(headArray);
      boolean isFailed = false;
      try {
        bodyType = deserializer.readByte();
        // LOG.info("body type: " + bodyType);
        contextName = deserializer.readUTF();
        workerID = deserializer.readInt();
      } catch (IOException e) {
        LOG.error(
          "Fail to decode body type, context name, "
            + "and worker ID", e);
        resetHead();
        isFailed = true;
      }
      if (!isFailed
        && (deserializer.getPos() < deserializer
          .getLength())) {
        try {
          operationName = deserializer.readUTF();
        } catch (IOException e) {
          LOG.error(
            "Fail to decode operation name", e);
          resetHead();
          isFailed = true;
        }
      }
      if (!isFailed
        && (deserializer.getPos() < deserializer
          .getLength())) {
        try {
          partitionID = deserializer.readInt();
        } catch (IOException e) {
          LOG.error(
            "Fail to decode partition ID", e);
          resetHead();
          isFailed = true;
        }
      }
      if (isFailed) {
        headStatus =
          DataStatus.ENCODED_ARRAY_DECODE_FAILED;
      } else {
        headStatus =
          DataStatus.ENCODED_ARRAY_DECODED;
      }
    }
    return headStatus;
  }

  private void resetHead() {
    bodyType = DataType.UNKNOWN_DATA_TYPE;
    contextName = null;
    workerID = Constants.UNKNOWN_WORKER_ID;
    operationName = null;
    partitionID = Constants.UNKNOWN_PARTITION_ID;
  }

  public DataStatus decodeBodyArray(
    ResourcePool pool) {
    return decodeAndNoDecompressBodyArray(pool);
    // return decodeAndDecompressBodyArray(pool);
  }

  private DataStatus
    decodeAndNoDecompressBodyArray(
      ResourcePool pool) {
    if (bodyStatus == DataStatus.ENCODED_ARRAY) {
      // If body status is encoded array, body
      // array cannot be null.
      if (bodyType == DataType.BYTE_ARRAY) {
        body = bodyArray;
      } else if (bodyType == DataType.INT_ARRAY) {
        body =
          DataUtils.deserializeBytesToIntArray(
            bodyArray, pool);
      } else if (bodyType == DataType.DOUBLE_ARRAY) {
        body =
          DataUtils
            .deserializeBytesToDoubleArray(
              bodyArray, pool);
      } else if (bodyType == DataType.STRUCT_OBJECT) {
        body =
          DataUtils
            .deserializeStructObjFromBytes(
              bodyArray, pool);
      } else if (bodyType == DataType.TRANS_LIST) {
        body =
          DataUtils.decodeTransList(bodyArray,
            pool);
      } else if (bodyType == DataType.PARTITION_LIST) {
        // long t1 = System.currentTimeMillis();
        body =
          PartitionUtils.decodePartitionList(
            bodyArray, pool);
        // long t2 = System.currentTimeMillis();
        // LOG.info("Decode partition list took "
        // + (t2 - t1));
      } else {
        LOG
          .error("Unknown Body Type. Cannot decode.");
      }
      if (body == null) {
        bodyStatus =
          DataStatus.ENCODED_ARRAY_DECODE_FAILED;
      } else {
        bodyStatus =
          DataStatus.ENCODED_ARRAY_DECODED;
      }
    }
    return bodyStatus;
  }

  private
    DataStatus
    decodeAndDecompressBodyArray(ResourcePool pool) {
    if (bodyStatus == DataStatus.ENCODED_ARRAY) {
      // If body status is encoded array, body
      // array cannot be null.
      // Get decompressed body array
      ByteArray originalBodyArray =
        getDecompressedByteArray(bodyArray, pool);
      if (originalBodyArray != null) {
        if (bodyType == DataType.BYTE_ARRAY) {
          body = originalBodyArray;
        } else if (bodyType == DataType.INT_ARRAY) {
          body =
            DataUtils.deserializeBytesToIntArray(
              originalBodyArray, pool);
        } else if (bodyType == DataType.DOUBLE_ARRAY) {
          body =
            DataUtils
              .deserializeBytesToDoubleArray(
                originalBodyArray, pool);
        } else if (bodyType == DataType.STRUCT_OBJECT) {
          body =
            DataUtils
              .deserializeStructObjFromBytes(
                originalBodyArray, pool);
        } else if (bodyType == DataType.TRANS_LIST) {
          body =
            DataUtils.decodeTransList(
              originalBodyArray, pool);
        } else if (bodyType == DataType.PARTITION_LIST) {
          body =
            PartitionUtils.decodePartitionList(
              originalBodyArray, pool);
        } else {
          LOG
            .error("Unknown Body Type. Cannot decode.");
        }
        // Release the decompressed byte array
        if (!(body instanceof ByteArray)) {
          pool.releaseBytes(originalBodyArray
            .getArray());
        }
        originalBodyArray = null;
      }
      if (body == null) {
        bodyStatus =
          DataStatus.ENCODED_ARRAY_DECODE_FAILED;
      } else {
        bodyStatus =
          DataStatus.ENCODED_ARRAY_DECODED;
      }
    }
    return bodyStatus;
  }

  private ByteArray getDecompressedByteArray(
    ByteArray byteArray, ResourcePool pool) {
    // long startTime =
    // System.currentTimeMillis();
    Deserializer deserializer =
      new Deserializer(byteArray.getArray(),
        byteArray.getStart(), byteArray.getSize());
    byte[] output = null;
    int originalLen = 0;
    try {
      originalLen = deserializer.readInt();
      output = pool.getBytes(originalLen);
      if (output == null) {
        return null;
      }
      // Decompress the byte array to the output
      Inflater decompresser = new Inflater();
      decompresser.setInput(byteArray.getArray(),
        4, byteArray.getSize() - 4);
      decompresser.inflate(output);
      decompresser.end();
      // LOG
      // .info("Decompression time spent: "
      // + (System.currentTimeMillis() -
      // startTime));
    } catch (Exception e) {
      LOG.error("Error in decompression", e);
      if (output != null) {
        pool.releaseBytes(output);
      }
    }
    return new ByteArray(output, 0, originalLen);
  }

  public DataStatus encodeHead(ResourcePool pool) {
    if (headStatus == DataStatus.DECODED) {
      // Encode fields to head array
      boolean isOpData = isOperationData();
      boolean isParData = isPartitionData();
      // bodyType, contextName, workerID
      int headArrSize =
        9 + 2 * contextName.length();
      if (isOpData) {
        headArrSize +=
          (4 + 2 * operationName.length());
      }
      if (isParData) {
        headArrSize += 4;
      }
      byte[] headBytes =
        pool.getBytes(headArrSize);
      headArray =
        new ByteArray(headBytes, 0, headArrSize);
      Serializer serializer =
        new Serializer(headArray);
      boolean isFailed = false;
      try {
        serializer.writeByte(bodyType);
        serializer.writeUTF(contextName);
        serializer.writeInt(workerID);
      } catch (Exception e) {
        LOG.error(
          "Fail to encode body type, context name"
            + "and worker ID.", e);
        isFailed = true;
      }
      if (!isFailed & isOpData) {
        try {
          serializer.writeUTF(operationName);
        } catch (Exception e) {
          LOG.error(
            "Fail to encode operation name.", e);
          isFailed = true;
        }
      }
      if (!isFailed & isParData) {
        try {
          serializer.writeInt(partitionID);
        } catch (Exception e) {
          LOG.error(
            "Fail to encode parititon ID.", e);
          isFailed = true;
        }
      }
      if (isFailed) {
        // Null is set for failed encoding
        // Notice that encoded part can be
        // released directly
        // or in encoding
        pool.releaseBytes(headBytes);
        headArray = null;
        headStatus =
          DataStatus.ENCODE_FAILED_DECODED;
      } else {
        headArray =
          new ByteArray(headBytes, 0, headArrSize);
        headStatus =
          DataStatus.ENCODED_ARRAY_DECODED;
      }
    }
    return headStatus;
  }

  public DataStatus encodeBody(ResourcePool pool) {
    return encodeAndNoCompressBody(pool);
    // return encodeAndCompressBody(pool);
  }

  private DataStatus encodeAndNoCompressBody(
    ResourcePool pool) {
    if (bodyStatus == DataStatus.DECODED) {
      if (bodyType == DataType.BYTE_ARRAY) {
        bodyArray = (ByteArray) body;
      } else if (bodyType == DataType.INT_ARRAY) {
        bodyArray =
          DataUtils.serializeIntArrayToByteArray(
            (IntArray) body, pool);
      } else if (bodyType == DataType.DOUBLE_ARRAY) {
        bodyArray =
          DataUtils
            .serializeDoubleArrayToByteArray(
              (DoubleArray) body, pool);
      } else if (bodyType == DataType.WRITABLE_OBJECT) {
        bodyStream =
          DataUtils
            .serializeWritableObjToByteStream(
              (WritableObject) body, pool);
      } else if (bodyType == DataType.STRUCT_OBJECT) {
        bodyArray =
          DataUtils
            .serializeStructObjToByteArray(
              (StructObject) body, pool);
      } else if (bodyType == DataType.TRANS_LIST) {
        bodyArray =
          DataUtils.encodeTransList(
            (List<Transferable>) body, pool);
      } else if (bodyType == DataType.PARTITION_LIST) {
        // long t1 = System.currentTimeMillis();
        bodyArray =
          PartitionUtils.encodePartitionList(
            (List<Partition>) body, pool);
        // long t2 = System.currentTimeMillis();
        // LOG.info("Encode partition list took "
        // + (t2 - t1));
      } else {
        System.out
          .println("Unknown body. Cannot encode.");
      }
      if (bodyArray != null) {
        bodyStatus =
          DataStatus.ENCODED_ARRAY_DECODED;
      } else if (bodyStream != null) {
        bodyStatus =
          DataStatus.ENCODED_STREAM_DECODED;
      } else {
        bodyStatus =
          DataStatus.ENCODE_FAILED_DECODED;
      }
    }
    return bodyStatus;
  }

  private DataStatus encodeAndCompressBody(
    ResourcePool pool) {
    if (bodyStatus == DataStatus.DECODED) {
      ByteArray originalBodyArray = null;
      if (bodyType == DataType.BYTE_ARRAY) {
        originalBodyArray = (ByteArray) body;
      } else if (bodyType == DataType.INT_ARRAY) {
        originalBodyArray =
          DataUtils.serializeIntArrayToByteArray(
            (IntArray) body, pool);
      } else if (bodyType == DataType.DOUBLE_ARRAY) {
        originalBodyArray =
          DataUtils
            .serializeDoubleArrayToByteArray(
              (DoubleArray) body, pool);
      } else if (bodyType == DataType.WRITABLE_OBJECT) {
        bodyStream =
          DataUtils
            .serializeWritableObjToByteStream(
              (WritableObject) body, pool);
      } else if (bodyType == DataType.STRUCT_OBJECT) {
        originalBodyArray =
          DataUtils
            .serializeStructObjToByteArray(
              (StructObject) body, pool);
      } else if (bodyType == DataType.TRANS_LIST) {
        originalBodyArray =
          DataUtils.encodeTransList(
            (List<Transferable>) body, pool);
      } else if (bodyType == DataType.PARTITION_LIST) {
        originalBodyArray =
          PartitionUtils.encodePartitionList(
            (List<Partition>) body, pool);
      } else {
        LOG.error("Unknown body. Cannot encode.");
      }
      if (originalBodyArray != null) {
        bodyArray =
          getCompressedByteArray(
            originalBodyArray, pool);
        // Release the array generated in the
        // first level serialization
        if (!(body instanceof ByteArray)) {
          pool.releaseBytes(originalBodyArray
            .getArray());
        }
        originalBodyArray = null;
      }
      if (bodyArray != null) {
        bodyStatus =
          DataStatus.ENCODED_ARRAY_DECODED;
      } else if (bodyStream != null) {
        bodyStatus =
          DataStatus.ENCODED_STREAM_DECODED;
      } else {
        bodyStatus =
          DataStatus.ENCODE_FAILED_DECODED;
      }
    }
    return bodyStatus;
  }

  private ByteArray getCompressedByteArray(
    ByteArray byteArray, ResourcePool pool) {
    long startTime = System.currentTimeMillis();
    int outputLen = 4 + byteArray.getSize();
    byte[] output = pool.getBytes(outputLen);
    if (output == null) {
      return null;
    }
    int compressedLen = 0;
    // Write original data length
    Serializer serializer =
      new Serializer(output, 0, output.length);
    try {
      serializer.writeInt(byteArray.getSize());
      // Compress the byte array to the output
      Deflater compresser = new Deflater();
      compresser
        .setInput(byteArray.getArray(),
          byteArray.getStart(),
          byteArray.getSize());
      compresser.finish();
      compressedLen =
        serializer.getPos()
          + compresser.deflate(
            output,
            serializer.getPos(),
            serializer.getLength()
              - serializer.getPos());
      compresser.end();
      if (byteArray.getSize() > 1000000) {
        LOG
          .info("Original byte array size: "
            + byteArray.getSize()
            + ", compressed byte array size: "
            + compressedLen
            + ", time spent: "
            + (System.currentTimeMillis() - startTime));
      }
    } catch (Exception e) {
      LOG.error("Error in compression", e);
      pool.releaseBytes(output);
      return null;
    }
    return new ByteArray(output, 0, compressedLen);
  }
}
