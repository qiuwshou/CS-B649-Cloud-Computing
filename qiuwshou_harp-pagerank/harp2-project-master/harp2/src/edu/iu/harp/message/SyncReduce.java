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

package edu.iu.harp.message;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.iu.harp.trans.StructObject;

/**
 * Each partition could have multi-destinations.
 * 
 * @author zhangbj
 *
 */
public class SyncReduce extends StructObject {

  private int workerID;
  private IntArrayList reduceList;

  public SyncReduce() {
    workerID = -1;
    reduceList = null;
  }

  public int getWorkerID() {
    return workerID;
  }

  public void setWorkerID(int workerID) {
    this.workerID = workerID;
  }

  public IntArrayList getReduceList() {
    return reduceList;
  }

  public void setReduceList(
    IntArrayList reduceList) {
    this.reduceList = reduceList;
  }

  @Override
  public int getSizeInBytes() {
    // WorkerID and reduce list size
    int size = 8;
    if (reduceList != null) {
      size += reduceList.size() * 4;
    }
    return size;
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    out.writeInt(workerID);
    if (reduceList != null) {
      out.writeInt(reduceList.size());
      for (int reduce : reduceList) {
        out.writeInt(reduce);
      }
    } else {
      out.writeInt(0);
    }
  }

  @Override
  public void read(DataInput in)
    throws IOException {
    workerID = in.readInt();
    int size = in.readInt();
    if (size > 0) {
      if (reduceList == null) {
        reduceList = new IntArrayList(size);
      }
      for (int i = 0; i < size; i++) {
        reduceList.add(in.readInt());
      }
    }
  }

  @Override
  public void clear() {
    workerID = -1;
    if (reduceList != null) {
      if (!reduceList.isEmpty()) {
        reduceList.clear();
      }
    }
  }
}
