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

import edu.iu.harp.partition.PartitionStatus;
import edu.iu.harp.trans.IntArray;

public class IntArrPlus extends
  ArrCombiner<IntArray> {

  @Override
  public PartitionStatus combine(
    ArrPartition<IntArray> curPar,
    ArrPartition<IntArray> newPar) {
    int[] ints1 = curPar.getArray().getArray();
    int size1 = curPar.getArray().getSize();
    int[] ints2 = newPar.getArray().getArray();
    int size2 = newPar.getArray().getSize();
    if (size1 != size2) {
      // throw new Exception("size1: " + size1
      // + ", size2: " + size2);
      return PartitionStatus.COMBINE_FAILED;
    }
    for (int i = 0; i < size2; i++) {
      ints1[i] += ints2[i];
    }
    return PartitionStatus.COMBINED;
  }
}
