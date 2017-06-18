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

package edu.iu.harp.primitivekv;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.log4j.Logger;

import edu.iu.harp.keyval.KeyValStatus;
import edu.iu.harp.utils.Int2ObjValHeldHashMap;

/**
 * Each value is a double array... We assume it is
 * not big, each of them just have few elements.
 * 
 * @author zhangbj
 * 
 */
public class Int2FltArrKVPartition extends
  AbsInt2ObjKVPartition<float[]> {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(Int2FltArrKVPartition.class);

  private int arrLen;

  public Int2FltArrKVPartition() {
    super();
    arrLen = -1;
  }

  public void initialize(int partitionID,
    int expectedKVCount, int arrLen) {
    super
      .initialize(partitionID, expectedKVCount);
    this.arrLen = arrLen;
  }

  public int getFloatArrLength() {
    return this.arrLen;
  }

  @Override
  public KeyValStatus putKeyVal(int key,
    float[] val) {
    if (!checkArrLen(val)) {
      return KeyValStatus.ADD_FAILED;
    }
    float[] floatArr =
      this.getKVMap().putIfAbsent(key, val);
    if (floatArr != null) {
      if (floatArr.length != val.length) {
        this.getKVMap().put(key, val);
        return KeyValStatus.ADDED;
      } else {
        System.arraycopy(val, 0, floatArr, 0,
          floatArr.length);
      }
      return KeyValStatus.COPIED;
    } else {
      return KeyValStatus.ADDED;
    }
  }

  private boolean checkArrLen(float[] val) {
    if (val.length != this.arrLen) {
      return false;
    }
    return true;
  }

  @Override
  public void clear() {
    this.getKVMap().clearKeys();
  }

  @Override
  public int getSizeInBytes() {
    // partition ID + arrLen + mapSize
    int size = 12;
    // Key ID + each element size
    size +=
      ((4 + this.arrLen * 4) * this.getKVMap()
        .size());
    return size;
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    out.writeInt(getPartitionID());
    out.writeInt(this.arrLen);
    out.writeInt(this.getKVMap().size());
    float[] floats = null;
    for (Int2ObjectMap.Entry<float[]> entry : this
      .getKVMap().int2ObjectEntrySet()) {
      out.writeInt(entry.getIntKey());
      floats = entry.getValue();
      for (int i = 0; i < this.arrLen; i++) {
        out.writeFloat(floats[i]);
      }
    }
  }

  @Override
  public void read(DataInput in)
    throws IOException {
    this.setPartitionID(in.readInt());
    this.arrLen = in.readInt();
    int size = in.readInt();
    if (this.getKVMap() == null) {
      this.createKVMap(size);
    }
    int key = 0;
    float[] floats = new float[this.arrLen];
    for (int i = 0; i < size; i++) {
      key = in.readInt();
      for (int j = 0; j < this.arrLen; j++) {
        floats[j] = in.readFloat();
      }
      KeyValStatus status =
        putKeyVal(key, floats);
      if (status == KeyValStatus.ADDED) {
        floats = new float[this.arrLen];
      }
    }
  }

  public void defragment() {
    // An alternative way is to do "trim"
    // this.getVertexMap().trim();
    Int2ObjValHeldHashMap<float[]> oldKVMap =
      this.getKVMap();
    this.createKVMap(oldKVMap.size());
    for (Int2ObjectMap.Entry<float[]> entry : oldKVMap
      .int2ObjectEntrySet()) {
      float[] newVal = new float[arrLen];
      System.arraycopy(entry.getValue(), 0,
        newVal, 0, arrLen);
      this.getKVMap().put(entry.getIntKey(),
        newVal);
    }
    oldKVMap.clear();
  }
}
