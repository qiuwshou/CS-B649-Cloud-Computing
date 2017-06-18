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
import edu.iu.harp.partition.PartitionStatus;
import edu.iu.harp.trans.Array;

/**
 * Combiner interface for combining a new array
 * partition to current array partition in the
 * table.
 * 
 * @author zhangbj
 * 
 * @param <A>
 *          New array partition
 */
public abstract class ArrCombiner<A extends Array<?>> {

  public abstract PartitionStatus
    combine(ArrPartition<A> curPar,
      ArrPartition<A> newPar);
}