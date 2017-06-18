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

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.log4j.Logger;

import edu.iu.harp.keyval.KeyValStatus;

/**
 * Each value is a double array... We assume it is
 * not big, each of them just have few elements.
 * 
 * @author zhangbj
 * 
 */
public class LongDblArrKVPartition extends
  AbsLong2ObjKVPartition<double[]> {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(LongDblArrKVPartition.class);

  private int arrLen;

  public LongDblArrKVPartition() {
    super();
    arrLen = -1;
  }

  public void initialize(int partitionID,
    int expectedVtxCount, int arrLen) {
    super.initialize(partitionID,
      expectedVtxCount);
    this.arrLen = arrLen;
  }

  public int getDoubleArrayLength() {
    return this.arrLen;
  }

  public KeyValStatus putKeyVal(long vertexID,
    double[] vertexVal) {
    if (!checkArrLen(vertexVal)) {
      return KeyValStatus.ADD_FAILED;
    }
    double[] doubleArr =
      this.getKVMap().putIfAbsent(vertexID,
        vertexVal);
    if (doubleArr != null) {
      if (doubleArr.length != vertexVal.length) {
        this.getKVMap().put(vertexID, vertexVal);
        return KeyValStatus.ADDED;
      } else {
        System.arraycopy(vertexVal, 0, doubleArr,
          0, doubleArr.length);
      }
      return KeyValStatus.COPIED;
    } else {
      return KeyValStatus.ADDED;
    }
  }

  private boolean checkArrLen(double[] val) {
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
    // partition Id + arrLen + mapSize
    int size = 12;
    // Key ID + each element size
    size +=
      ((8 + this.arrLen * 8) * this.getKVMap()
        .size());
    return size;
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    out.writeInt(getPartitionID());
    out.writeInt(this.arrLen);
    out.writeInt(this.getKVMap().size());
    double[] doubles = null;
    for (Long2ObjectMap.Entry<double[]> entry : this
      .getKVMap().long2ObjectEntrySet()) {
      out.writeLong(entry.getLongKey());
      doubles = entry.getValue();
      for (int i = 0; i < this.arrLen; i++) {
        out.writeDouble(doubles[i]);
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
      createKVMap(size);
    }
    long key = 0;
    double[] doubles = new double[this.arrLen];
    for (int i = 0; i < size; i++) {
      key = in.readLong();
      for (int j = 0; j < this.arrLen; j++) {
        doubles[j] = in.readDouble();
      }
      KeyValStatus status =
        putKeyVal(key, doubles);
      if (status == KeyValStatus.ADDED) {
        doubles = new double[this.arrLen];
      }
    }
  }
}
