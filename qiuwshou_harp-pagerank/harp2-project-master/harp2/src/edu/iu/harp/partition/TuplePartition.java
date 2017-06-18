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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import edu.iu.harp.io.Constants;
import edu.iu.harp.io.Deserializer;
import edu.iu.harp.io.Serializer;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.ByteArray;
import edu.iu.harp.trans.StructObject;

/**
 * Allow write, read, remove, add
 * 
 * 
 * @author zhangbj
 *
 */
public class TuplePartition implements Partition {

  protected static final Logger LOG = Logger
    .getLogger(TuplePartition.class);

  private final int partitionID;
  private ArrayList<ByteArray> arrays;

  // [0] array index [1] element index
  // Stop at the end of last
  // writing index
  private int[] writePos;
  // Stop at the beginning of current reading
  // index if we have current tuple,
  // otherwise stop at the beginning of next
  // reading
  private int[] readPos;
  private boolean isReadPosDefault;

  private Class<StructObject>[] tupleClass;
  private boolean hasTupleClass;
  private StructObject[] curTuple;
  private boolean hasCurTuple;

  public TuplePartition(int partitionID,
    Class<StructObject>[] classes) {
    this.partitionID = partitionID;
    arrays = new ArrayList<>();
    writePos = new int[2];
    writePos[0] = 0;
    writePos[1] = 0;
    readPos = new int[2];
    readPos[0] = 0;
    readPos[1] = 0;
    isReadPosDefault = true;
    curTuple = null;
    hasCurTuple = false;
    setTupleClass(classes);
  }

  @Override
  public int getPartitionID() {
    return partitionID;
  }

  private boolean setTupleClass(
    Class<StructObject>[] classes) {
    if (hasTupleClass) {
      return false;
    } else {
      for (int i = 0; i < classes.length; i++) {
        if (classes[i] == null) {
          return false;
        }
      }
      this.tupleClass = classes;
      this.hasTupleClass = true;
      this.curTuple =
        new StructObject[tupleClass.length];
      try {
        for (int i = 0; i < tupleClass.length; i++) {
          // Create objects directly, not through
          // the resource pool
          this.curTuple[i] =
            ResourcePool
              .createObject(tupleClass[i]);
        }
        hasCurTuple = false;
      } catch (Exception e) {
        LOG.error(
          "Fail to initialize current tuple.", e);
        Arrays.fill(curTuple, null);
        hasCurTuple = false;
      }
      return true;
    }
  }

  public boolean hasTupleClass() {
    return hasTupleClass;
  }

  public Class<StructObject>[] getTupleClass() {
    return this.tupleClass;
  }

  public void addByteArray(ByteArray array,
    boolean isArrEmpty) {
    if (isArrEmpty) {
      // Empty array is write only
      arrays.add(array);
    } else {
      // An array with contents
      arrays.add(0, array);
      writePos[0]++;
      defaultReadPos();
    }
  }

  public boolean isEmpty() {
    return this.arrays.isEmpty();
  }

  public int getNumByteArrays() {
    return arrays.size();
  }

  public List<ByteArray> getByteArrays() {
    return arrays;
  }

  public List<ByteArray> removeByteArrays() {
    List<ByteArray> rmArays = arrays;
    arrays = new ArrayList<>();
    writePos[0] = 0;
    writePos[0] = 0;
    defaultReadPos();
    return rmArays;
  }

  public void defaultReadPos() {
    if (!isReadPosDefault) {
      readPos[0] = 0;
      readPos[1] = 0;
      isReadPosDefault = true;
      hasCurTuple = false;
    }
  }

  /**
   * Finalize byte arrays before sending
   * 
   * @return empty arrays has not been written
   */
  public void
    finalizeByteArrays(ResourcePool pool) {
    // reset read pos to 0
    defaultReadPos();
    // Set the last byte array with the size to
    // the write pos
    if (writePos[0] < arrays.size()) {
      ByteArray oldArray =
        arrays.remove(writePos[0]);
      ByteArray newArray =
        new ByteArray(oldArray.getArray(),
          oldArray.getStart(), writePos[1]);
      arrays.add(writePos[0], newArray);
      writePos[0]++;
      writePos[1] = 0;
      // Remove empty arrays
      for (int i = writePos[0]; i < arrays.size(); i++) {
        pool.releaseBytes(arrays.remove(i)
          .getArray());
      }
    }
  }

  private int getTupleSize(StructObject[] tuple) {
    int size = 0;
    for (int i = 0; i < tuple.length; i++) {
      if (tuple[i] != null) {
        size += tuple[i].getSizeInBytes();
      } else {
        return Constants.UNKNOWN_TUPLE_SIZE;
      }
    }
    return size;
  }

  public boolean hasSpaceToWrite() {
    if (writePos[0] < arrays.size()) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Write pos stops at the last writing position.
   * 
   * @param objs
   * @return
   */
  public boolean writeTuple(StructObject... objs) {
    // No tuple class
    if (!hasTupleClass) {
      return false;
    }
    // Check if the objs have valid sizes
    int tupleSize = getTupleSize(objs);
    // LOG.info("Write tuple. size: " +
    // tupleSize);
    if (tupleSize == Constants.UNKNOWN_TUPLE_SIZE) {
      return false;
    }
    // LOG.info("Write pos " + writePos[0] + " "
    // + writePos[1]);
    while (true) {
      if (writePos[0] == arrays.size()) {
        return false;
      } else {
        ByteArray array = arrays.get(writePos[0]);
        if ((writePos[1] + tupleSize) > array
          .getSize()) {
          // Cannot continue writing
          // Adjust byte array size
          ByteArray oldArray =
            arrays.remove(writePos[0]);
          ByteArray newArray =
            new ByteArray(oldArray.getArray(),
              oldArray.getStart(), writePos[1]);
          arrays.add(writePos[0], newArray);
          // Move to next pos for writing
          writePos[0]++;
          writePos[1] = 0;
        } else {
          // Get a valid pos for writing
          break;
        }
      }
    }
    // LOG.info("Adjusted write pos " +
    // writePos[0]
    // + " " + writePos[1]);
    ByteArray array = arrays.get(writePos[0]);
    boolean isFailed = false;
    Serializer serializer =
      new Serializer(array.getArray(),
        array.getStart() + writePos[1],
        array.getStart() + array.getSize());
    try {
      for (StructObject obj : objs) {
        obj.write(serializer);
      }
    } catch (Exception e) {
      LOG.error(
        "Error when writing struct objects", e);
      isFailed = true;
    }
    if (!isFailed) {
      writePos[1] += tupleSize;
      // LOG.info("Write end pos " + writePos[0] +
      // " "
      // + writePos[1]);
      return true;
    } else {
      return false;
    }
  }

  public boolean hasSpaceToRead() {
    if (readPos[0] < writePos[0]) {
      return true;
    } else if (readPos[0] == writePos[0]
      && readPos[1] < writePos[1]) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * If current tuple exists, read pos stops at
   * the beginning of the current reading,
   * otherwise it stops at the next reading
   * position
   * 
   * @return
   */
  public boolean readTuple() {
    // No tuple class
    if (!hasTupleClass) {
      return false;
    }
    // Go to the next place for reading
    if (hasCurTuple) {
      int tupleSize = getTupleSize(curTuple);
      // LOG.info("Tuple Size: " + tupleSize);
      if (tupleSize != Constants.UNKNOWN_TUPLE_SIZE) {
        readPos[1] += tupleSize;
        isReadPosDefault = false;
      }
      hasCurTuple = false;
    }
    // LOG.info("Read Pos " + readPos[0] + " "
    // + readPos[1]);
    // If it is at the end of an array, go to the
    // next array.
    while (true) {
      if (readPos[0] == writePos[0]
        && readPos[1] >= writePos[1]) {
        return false;
      } else {
        ByteArray array =
          this.arrays.get(readPos[0]);
        if (readPos[1] == array.getSize()) {
          readPos[0]++;
          readPos[1] = 0;
          if (readPos[0] == arrays.size()) {
            return false;
          }
        } else {
          break;
        }
      }
    }
    // LOG.info("Adjusted Read Pos " + readPos[0]
    // + " " + readPos[1]);
    ByteArray array = this.arrays.get(readPos[0]);
    Deserializer deserializer =
      new Deserializer(array.getArray(),
        array.getStart() + readPos[1],
        array.getStart() + array.getSize());
    try {
      for (int i = 0; i < curTuple.length; i++) {
        curTuple[i].read(deserializer);
      }
      hasCurTuple = true;
    } catch (Exception e) {
      LOG.error("Error when read", e);
      // Jump read pos to next array
      // Or to the place where write pos is
      // e.printStackTrace();
      if (readPos[0] < writePos[0]) {
        readPos[0]++;
        readPos[1] = 0;
      } else if (readPos[0] == writePos[0]) {
        readPos[1] = writePos[1];
      }
      hasCurTuple = false;
      return false;
    }
    return true;
  }

  /**
   * get the current tuple
   * 
   * @return
   */
  public StructObject[] getTuple() {
    if (hasCurTuple) {
      return this.curTuple;
    } else {
      return null;
    }
  }

  public boolean removeTuple() {
    // If no current edge
    if (!hasCurTuple) {
      return false;
    }
    int tupleSize = getTupleSize(this.curTuple);
    if (tupleSize == Constants.UNKNOWN_TUPLE_SIZE) {
      return false;
    }
    ByteArray array = this.arrays.get(readPos[0]);
    int tupleEnd = readPos[1] + tupleSize;
    if (tupleEnd == array.getSize()) {
      // If reach the end of the array
      ByteArray oldArray =
        this.arrays.remove(readPos[0]);
      ByteArray newArray =
        new ByteArray(oldArray.getArray(),
          oldArray.getStart(), readPos[1]);
      this.arrays.add(readPos[0], newArray);
      // If write and read are on the same array
      // Write pos must be at the end of the
      // original array
      if (writePos[0] == readPos[0]) {
        writePos[1] = readPos[1];
      }
      hasCurTuple = false;
      return true;
    } else if (readPos[1] == 0) {
      // If it is at the beginning of the array
      ByteArray oldArray =
        this.arrays.remove(readPos[0]);
      ByteArray newArray =
        new ByteArray(oldArray.getArray(),
          oldArray.getStart() + tupleEnd,
          oldArray.getSize() - tupleSize);
      this.arrays.add(readPos[0], newArray);
      // readPos[0] is the same
      // readPos[1] should still be 0
      // writePos should be ahead of readPos
      // the distance between newStart and
      // original start is tupleSize
      if (writePos[0] == readPos[0]) {
        writePos[1] -= tupleSize;
      }
      hasCurTuple = false;
      return true;
    } else {
      // In other cases, the tuple is at the
      // middle of the array
      ByteArray oldArray =
        this.arrays.remove(readPos[0]);
      ByteArray newArray1 =
        new ByteArray(oldArray.getArray(),
          oldArray.getStart(), readPos[1]);
      this.arrays.add(readPos[0], newArray1);
      ByteArray newArray2 =
        new ByteArray(oldArray.getArray(),
          oldArray.getStart() + tupleEnd,
          oldArray.getSize() - tupleEnd);
      this.arrays.add(readPos[0] + 1, newArray2);
      if (writePos[0] == readPos[0]) {
        writePos[0]++;
        writePos[1] -= tupleEnd;
      }
      hasCurTuple = false;
      return true;
    }
  }
}
