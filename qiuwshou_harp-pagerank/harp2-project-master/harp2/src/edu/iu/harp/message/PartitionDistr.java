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

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.iu.harp.io.Constants;
import edu.iu.harp.trans.StructObject;

public class PartitionDistr extends StructObject {
  private int workerID;
  private Int2IntOpenHashMap parIDCount;

  public PartitionDistr() {
    workerID = Constants.UNKNOWN_WORKER_ID;
    parIDCount = null;
  }

  public void setWorkerID(int id) {
    this.workerID = id;
  }

  public int getWorkerID() {
    return workerID;
  }

  public void setParDistr(
    Int2IntOpenHashMap partitionDistr) {
    parIDCount = partitionDistr;
  }

  public Int2IntOpenHashMap getParDistr() {
    return parIDCount;
  }

  @Override
  public int getSizeInBytes() {
    // Worker ID + map size
    if (parIDCount != null) {
      return 8 + parIDCount.size() * 8;
    } else {
      return 8;
    }
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    out.writeInt(workerID);
    if (parIDCount != null) {
      out.writeInt(parIDCount.size());
      ObjectIterator<Int2IntMap.Entry> iterator =
        parIDCount.int2IntEntrySet()
          .fastIterator();
      while (iterator.hasNext()) {
        Int2IntMap.Entry entry = iterator.next();
        out.writeInt(entry.getIntKey());
        out.writeInt(entry.getIntValue());
      }
    } else {
      out.writeInt(0);
    }
  }

  @Override
  public void read(DataInput in)
    throws IOException {
    this.workerID = in.readInt();
    int size = in.readInt();
    if (size > 0) {
      if (parIDCount == null) {
        parIDCount = new Int2IntOpenHashMap(size);
        parIDCount.defaultReturnValue(0);
      }
      for (int i = 0; i < size; i++) {
        parIDCount
          .put(in.readInt(), in.readInt());
      }
    }
  }

  @Override
  public void clear() {
    if (parIDCount != null) {
      parIDCount.clear();
    }
  }
}
