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

package edu.iu.harp.array;

import edu.iu.harp.array.ArrPartition;
import edu.iu.harp.partition.PartitionFunction;
import edu.iu.harp.trans.DoubleArray;

public class Double2DArrAvg extends
  PartitionFunction<ArrPartition<DoubleArray>> {

  private int rowLen;

  public Double2DArrAvg(int rowLen) {
    this.rowLen = rowLen;
  }

  @Override
  public void apply(
    ArrPartition<DoubleArray> partition)
    throws Exception {
    // array[0, 0 + rowLen, ...] row count
    double[] doubles =
      partition.getArray().getArray();
    int size = partition.getArray().getSize();
    if (size % rowLen != 0) {
      throw new Exception();
    }
    for (int i = 0; i < size; i += this.rowLen) {
      for (int j = 1; j < this.rowLen; j++) {
        // Calculate avg
        if (doubles[i] != 0) {
          doubles[i + j] /= doubles[i];
        }
      }
    }
  }
}
