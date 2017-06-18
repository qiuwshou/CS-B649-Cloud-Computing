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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.iu.harp.event.Event;
import edu.iu.harp.event.EventType;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.ByteArray;
import edu.iu.harp.trans.DoubleArray;
import edu.iu.harp.trans.IntArray;
import edu.iu.harp.trans.StructObject;
import edu.iu.harp.trans.Transferable;
import edu.iu.harp.trans.WritableObject;

public class DataUtils {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(DataUtils.class);

  public static ByteArray deserializeByteArray(
    Deserializer din, ResourcePool pool) {
    byte[] bytes = null;
    int byteArrSize = 0;
    try {
      byteArrSize = din.readInt();
    } catch (Exception e) {
      LOG.error("Fail to deserialize byte array",
        e);
      return null;
    }
    bytes = pool.getBytes(byteArrSize);
    try {
      din.readFully(bytes, 0, byteArrSize);
    } catch (Exception e) {
      LOG.error("Fail to deserialize byte array",
        e);
      pool.releaseBytes(bytes);
      bytes = null;
      return null;
    }
    return new ByteArray(bytes, 0, byteArrSize);
  }

  public static IntArray
    deserializeBytesToIntArray(
      ByteArray byteArray, ResourcePool pool) {
    Deserializer din =
      new Deserializer(byteArray);
    return deserializeIntArray(din, pool);
  }

  public static IntArray deserializeIntArray(
    Deserializer din, ResourcePool pool) {
    int[] ints = null;
    int intsSize = 0;
    try {
      intsSize = din.readInt();
    } catch (Exception e) {
      return null;
    }
    ints = pool.getInts(intsSize);
    try {
      for (int i = 0; i < intsSize; i++) {
        ints[i] = din.readInt();
      }
    } catch (Exception e) {
      pool.releaseInts(ints);
      ints = null;
      return null;
    }
    return new IntArray(ints, 0, intsSize);
  }

  public static DoubleArray
    deserializeBytesToDoubleArray(
      ByteArray byteArray, ResourcePool pool) {
    Deserializer din =
      new Deserializer(byteArray);
    return deserializeDoubleArray(din, pool);
  }

  public static DoubleArray
    deserializeDoubleArray(Deserializer din,
      ResourcePool pool) {
    double[] doubles = null;
    int doublesSize = 0;
    try {
      doublesSize = din.readInt();
    } catch (IOException e) {
      LOG.error(
        "Fail to deserialize double array", e);
      return null;
    }
    // LOG.info("Recv doubleArrSize: " +
    // doublesSize);
    doubles = pool.getDoubles(doublesSize);
    if (doubles == null) {
      return null;
    }
    try {
      for (int i = 0; i < doublesSize; i++) {
        doubles[i] = din.readDouble();
      }
    } catch (Exception e) {
      LOG.error(
        "Fail to deserialize double array", e);
      pool.releaseDoubles(doubles);
      doubles = null;
      return null;
    }
    return new DoubleArray(doubles, 0,
      doublesSize);
  }

  public static ByteArray
    serializeIntArrayToByteArray(
      IntArray intArray, ResourcePool pool) {
    int byteArrSize = intArray.getSize() * 4 + 4;
    byte[] bytes = pool.getBytes(byteArrSize);
    ByteArray byteArray =
      new ByteArray(bytes, 0, byteArrSize);
    DataOutput dataOut =
      new Serializer(byteArray);
    try {
      serializeIntArray(intArray, dataOut);
    } catch (Exception e) {
      pool.releaseBytes(bytes);
      byteArray = null;
    }
    return byteArray;
  }

  public static void serializeIntArray(
    IntArray intArray, DataOutput dataOut)
    throws Exception {
    int[] ints = intArray.getArray();
    int intArrStart = intArray.getStart();
    int intArrSize = intArray.getSize();
    int intArrLen = intArrStart + intArrSize;
    dataOut.writeInt(intArrSize);
    for (int i = intArrStart; i < intArrLen; i++) {
      dataOut.writeInt(ints[i]);
    }
  }

  public static ByteArray
    serializeDoubleArrayToByteArray(
      DoubleArray doubleArray, ResourcePool pool) {
    int byteArrSize =
      doubleArray.getSize() * 8 + 4;
    byte[] bytes = pool.getBytes(byteArrSize);
    ByteArray byteArray =
      new ByteArray(bytes, 0, byteArrSize);
    DataOutput dataOut =
      new Serializer(byteArray);
    try {
      serializeDoubleArray(doubleArray, dataOut);
    } catch (Exception e) {
      pool.releaseBytes(bytes);
      byteArray = null;
    }
    return byteArray;
  }

  public static void serializeDoubleArray(
    DoubleArray doubleArray, DataOutput dataOut)
    throws Exception {
    double[] doubles = doubleArray.getArray();
    int doubleArrStart = doubleArray.getStart();
    int doubleArrSize = doubleArray.getSize();
    int doubleArrLen =
      doubleArrStart + doubleArrSize;
    // LOG.info("Send doubleArrSize: " +
    // doubleArrSize);
    dataOut.writeInt(doubleArrSize);
    for (int i = doubleArrStart; i < doubleArrLen; i++) {
      dataOut.writeDouble(doubles[i]);
    }
  }

  public static int getStructObjSizeInBytes(
    StructObject object) {
    // class name + size in bytes
    return (object.getSizeInBytes()
      + object.getClass().getName().length() * 2 + 4);
  }

  public static ByteArrayOutputStream
    serializeWritableObjToByteStream(
      WritableObject object, ResourcePool pool) {
    String className =
      object.getClass().getName();
    // LOG.info("Class name: "
    // + obj.getClass().getName());
    ByteArrayOutputStream byteStream =
      pool.getByteArrayOutputStreamPool()
        .getByteArrayOutputStream();
    DataOutputStream dataOut =
      new DataOutputStream(byteStream);
    try {
      dataOut.writeUTF(className);
      object.write(dataOut);
      dataOut.flush();
    } catch (Exception e) {
      pool.getByteArrayOutputStreamPool()
        .releaseByteArrayOutputStreamInUse(
          byteStream);
      byteStream = null;
    }
    // LOG.info("Serialized writable object size: "
    // + byteOut.size());
    return byteStream;
  }

  public static WritableObject
    deserializeWritableObjFromBytes(
      ByteArray byteArray, ResourcePool pool) {
    String className = null;
    WritableObject obj = null;
    DataInputStream din =
      new DataInputStream(
        new ByteArrayInputStream(
          byteArray.getArray()));
    try {
      className = din.readUTF();
      // LOG.info("Class name: " + className);
      obj = pool.getWritableObject(className);
    } catch (Exception e) {
      obj = null;
    }
    if (obj == null) {
      return null;
    }
    try {
      obj.read(din);
    } catch (Exception e) {
      pool.releaseWritableObject(obj);
    }
    return obj;
  }

  public static ByteArray
    serializeStructObjToByteArray(
      StructObject object, ResourcePool pool) {
    String className =
      object.getClass().getName();
    int byteArrSize =
      getStructObjSizeInBytes(object);
    // LOG
    // .info("Serialize struct object class name: "
    // + className
    // + ", size in bytes: "
    // + byteArrSize);
    byte[] bytes = pool.getBytes(byteArrSize);
    ByteArray byteArray =
      new ByteArray(bytes, 0, byteArrSize);
    DataOutput dataOut =
      new Serializer(byteArray);
    try {
      serializeStructObject(className, object,
        dataOut);
    } catch (Exception e) {
      LOG.error(
        "Fail to serialize struct object with class name: "
          + object.getClass().getName(), e);
      pool.releaseBytes(bytes);
      byteArray = null;
    }
    return byteArray;
  }

  public static void serializeStructObject(
    StructObject object, DataOutput dataOut)
    throws Exception {
    String className =
      object.getClass().getName();
    serializeStructObject(className, object,
      dataOut);
  }

  public static void serializeStructObject(
    String className, StructObject object,
    DataOutput dataOut) throws Exception {
    dataOut.writeUTF(className);
    object.write(dataOut);
  }

  public static StructObject
    deserializeStructObjFromBytes(
      ByteArray byteArray, ResourcePool pool) {
    DataInput din = new Deserializer(byteArray);
    return deserializeStructObj(din, pool);
  }

  public static StructObject
    deserializeStructObj(DataInput din,
      ResourcePool pool) {
    String className = null;
    StructObject obj = null;
    try {
      className = din.readUTF();
    } catch (Exception e) {
      LOG.error(
        "Fail to deserialize the class name "
          + "of struct object", e);
      return null;
    }
    obj =
      (StructObject) pool
        .getWritableObject(className);
    if (obj == null) {
      return obj;
    }
    try {
      obj.read(din);
    } catch (Exception e) {
      LOG
        .error(
          "Fail to deserialize struct object with class name "
            + className, e);
      pool.releaseWritableObject(obj);
      obj = null;
    }
    return obj;
  }

  /**
   * Serialize multiple transferables with
   * different data types
   * 
   * @param objs
   * @param pool
   * @return
   */
  public static ByteArray encodeTransList(
    List<Transferable> objs, ResourcePool pool) {
    // Get size in bytes
    int size = 0;
    for (Transferable obj : objs) {
      if (obj instanceof ByteArray) {
        size += (5 + ((ByteArray) obj).getSize());
      } else if (obj instanceof IntArray) {
        size +=
          (5 + ((IntArray) obj).getSize() * 4);
      } else if (obj instanceof DoubleArray) {
        size +=
          (5 + ((DoubleArray) obj).getSize() * 8);
      } else if (obj instanceof StructObject) {
        size +=
          (1 + DataUtils
            .getStructObjSizeInBytes((StructObject) obj));
      }
    }
    if (size == 0) {
      size = 1;
    }
    byte[] bytes = pool.getBytes(size);
    ByteArray byteArray =
      new ByteArray(bytes, 0, size);
    Serializer serializer =
      new Serializer(byteArray);
    try {
      if (size == 1) {
        serializer
          .writeByte(DataType.UNKNOWN_DATA_TYPE);
      } else {
        for (Transferable obj : objs) {
          if (obj instanceof ByteArray) {
            serializer
              .writeByte(DataType.BYTE_ARRAY);
            ByteArray array = (ByteArray) obj;
            serializer.writeInt(byteArray
              .getSize());
            serializer.write(array.getArray(),
              array.getStart(), array.getSize());
          } else if (obj instanceof IntArray) {
            serializer
              .writeByte(DataType.INT_ARRAY);
            IntArray array = (IntArray) obj;
            DataUtils.serializeIntArray(array,
              serializer);
          } else if (obj instanceof DoubleArray) {
            serializer
              .writeByte(DataType.DOUBLE_ARRAY);
            DoubleArray array = (DoubleArray) obj;
            DataUtils.serializeDoubleArray(array,
              serializer);
          } else if (obj instanceof StructObject) {
            serializer
              .writeByte(DataType.STRUCT_OBJECT);
            StructObject structObj =
              (StructObject) obj;
            // structObj.write(serializer);
            DataUtils.serializeStructObject(
              structObj, serializer);
          }
        }
      }
    } catch (Exception e) {
      LOG.error(
        "Fail to encode transferable list.", e);
      pool.releaseBytes(bytes);
      bytes = null;
      byteArray = null;
      serializer = null;
    }
    return byteArray;
  }

  public static List<Transferable>
    decodeTransList(final ByteArray byteArray,
      final ResourcePool pool) {
    List<Transferable> objs = new LinkedList<>();
    Deserializer deserializer =
      new Deserializer(byteArray);
    byte dataType = DataType.UNKNOWN_DATA_TYPE;
    boolean isFailed = false;
    while (deserializer.getPos() < deserializer
      .getLength()) {
      try {
        dataType = deserializer.readByte();
      } catch (Exception e) {
        isFailed = true;
        break;
      }
      if (dataType == DataType.UNKNOWN_DATA_TYPE) {
        break;
      }
      Transferable obj = null;
      if (dataType == DataType.BYTE_ARRAY) {
        ByteArray byteArr =
          DataUtils.deserializeByteArray(
            deserializer, pool);
        if (byteArr != null) {
          obj = byteArr;
        } else {
          obj = null;
        }
      } else if (dataType == DataType.INT_ARRAY) {
        IntArray intArr =
          DataUtils.deserializeIntArray(
            deserializer, pool);
        if (intArr != null) {
          obj = intArr;
        } else {
          obj = null;
        }
      } else if (dataType == DataType.DOUBLE_ARRAY) {
        DoubleArray doubleArr =
          DataUtils.deserializeDoubleArray(
            deserializer, pool);
        if (doubleArr != null) {
          obj = doubleArr;
        } else {
          obj = null;
        }
      } else if (dataType == DataType.STRUCT_OBJECT) {
        StructObject structObj =
          DataUtils.deserializeStructObj(
            deserializer, pool);
        if (structObj != null) {
          obj = structObj;
        } else {
          obj = null;
        }
      } else {
        LOG.info("Unkown data type.");
      }
      if (obj == null) {
        isFailed = true;
        break;
      } else {
        objs.add(obj);
      }
    }
    if (isFailed) {
      // Clean
      for (Transferable obj : objs) {
        releaseTrans(pool, obj);
      }
      return null;
    } else {
      return objs;
    }

  }

  public static void releaseTrans(
    ResourcePool pool, Transferable trans) {
    if (trans instanceof ByteArray) {
      pool.releaseBytes(((ByteArray) trans)
        .getArray());
    } else if (trans instanceof IntArray) {
      pool.releaseInts(((IntArray) trans)
        .getArray());
    } else if (trans instanceof DoubleArray) {
      pool.releaseDoubles(((DoubleArray) trans)
        .getArray());
    } else if (trans instanceof StructObject) {
      pool
        .releaseWritableObject((StructObject) trans);
    }
  }

  public static void
    releaseTrans(ResourcePool pool,
      List<? extends Transferable> trans,
      byte type) {
    if (type == DataType.BYTE_ARRAY) {
      for (Transferable tran : trans) {
        pool.releaseBytes(((ByteArray) tran)
          .getArray());
      }
    } else if (type == DataType.INT_ARRAY) {
      for (Transferable tran : trans) {
        pool.releaseInts(((IntArray) tran)
          .getArray());
      }
    } else if (type == DataType.DOUBLE_ARRAY) {
      for (Transferable tran : trans) {
        pool.releaseDoubles(((DoubleArray) tran)
          .getArray());
      }
    } else if (type == DataType.STRUCT_OBJECT) {
      for (Transferable tran : trans) {
        pool
          .releaseWritableObject((StructObject) tran);
      }
    }
  }

  public static void addDataToQueueOrMap(
    EventQueue eventQueue, EventType eventType,
    DataMap dataMap, Data data) {
    // LOG.info("Add data - " + "context name: "
    // + data.getContextName() + ", worker ID: "
    // + data.getWorkerID() + ", operation name: "
    // + data.getOperationName() + ", body type: "
    // + data.getBodyType()
    // + ", isOperationData: "
    // + data.isOperationData() + ", isData: "
    // + data.isData() + ", head status: "
    // + data.getHeadStatus() + ", body status: "
    // + data.getBodyStatus());
    if ((data.getHeadStatus() == DataStatus.DECODED || data
      .getHeadStatus() == DataStatus.ENCODED_ARRAY_DECODED)
      && (data.getBodyStatus() == DataStatus.ENCODED_ARRAY || data
        .getBodyStatus() == DataStatus.DECODED)) {
      if (data.isOperationData()) {
        dataMap.putData(data);
      } else if (data.isData()) {
        if (data.getBodyType() == DataType.TRANS_LIST) {
          List<Transferable> objs =
            (List<Transferable>) data.getBody();
          for (Transferable obj : objs) {
            eventQueue.addEvent(new Event(
              eventType, data.getContextName(),
              data.getWorkerID(), obj));
          }
        } else if (data.getBodyType() == DataType.BYTE_ARRAY) {
          eventQueue.addEvent(new Event(
            eventType, data.getContextName(),
            data.getWorkerID(), (ByteArray) data
              .getBody()));
        } else if (data.getBodyType() == DataType.INT_ARRAY) {
          eventQueue.addEvent(new Event(
            eventType, data.getContextName(),
            data.getWorkerID(), (IntArray) data
              .getBody()));
        } else if (data.getBodyType() == DataType.DOUBLE_ARRAY) {
          eventQueue.addEvent(new Event(
            eventType, data.getContextName(),
            data.getWorkerID(),
            (DoubleArray) data.getBody()));
        } else if (data.getBodyType() == DataType.STRUCT_OBJECT) {
          eventQueue.addEvent(new Event(
            eventType, data.getContextName(),
            data.getWorkerID(),
            (StructObject) data.getBody()));
        }
      }
    }
  }
}
