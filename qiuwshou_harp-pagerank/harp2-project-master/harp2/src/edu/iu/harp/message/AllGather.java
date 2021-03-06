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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.iu.harp.trans.StructObject;

public class AllGather extends StructObject {

  private int numPartition;

  public AllGather() {
    numPartition = 0;
  }

  public int getNumPartitions() {
    return this.numPartition;
  }

  public void setNumPartitions(int num) {
    this.numPartition = num;
  }

  public void addPartitionNum(int num) {
    this.numPartition += num;
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    out.writeInt(this.numPartition);
  }

  @Override
  public void read(DataInput in)
    throws IOException {
    this.numPartition = in.readInt();
  }

  @Override
  public int getSizeInBytes() {
    return 4;
  }

  public void clear() {
    numPartition = 0;
  }
}
