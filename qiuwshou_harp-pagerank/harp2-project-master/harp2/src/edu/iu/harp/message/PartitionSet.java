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

import edu.iu.harp.io.Constants;
import edu.iu.harp.trans.StructObject;

public class PartitionSet extends StructObject {
  private int workerID;
  private IntArrayList parIDs;

  public PartitionSet() {
    workerID = Constants.UNKNOWN_WORKER_ID;
    parIDs = null;
  }

  public void setWorkerID(int id) {
    this.workerID = id;
  }

  public int getWorkerID() {
    return workerID;
  }

  public void
    setParSet(IntArrayList partitionSet) {
    parIDs = partitionSet;
  }

  public IntArrayList getParSet() {
    return parIDs;
  }

  @Override
  public int getSizeInBytes() {
    // Worker ID + map size
    if (parIDs != null) {
      return 8 + parIDs.size() * 4;
    } else {
      return 8;
    }
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    out.writeInt(workerID);
    if (parIDs != null) {
      out.writeInt(parIDs.size());
      for (int i = 0; i < parIDs.size(); i++) {
        out.writeInt(parIDs.getInt(i));
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
      if (parIDs == null) {
        parIDs = new IntArrayList(size);
      }
      for (int i = 0; i < size; i++) {
        parIDs.add(in.readInt());
      }
    }
  }

  @Override
  public void clear() {
    parIDs = null;
  }
}
