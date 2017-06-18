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

import org.apache.log4j.Logger;

import edu.iu.harp.partition.PartitionStatus;
import edu.iu.harp.partition.Table;
import edu.iu.harp.trans.Array;

/**
 * Array table, which includes array partitions.
 * When there is array partition ID conflicts, use
 * array combiner to combine two array partitions.
 * 
 * @author zhangbj
 * 
 * @param <A>
 *          Array type
 * @param <C>
 *          Array combiner type
 */
public class ArrTable<A extends Array<?>> extends
  Table<ArrPartition<A>> {

  private static final Logger LOG = Logger
    .getLogger(ArrTable.class);

  private final ArrCombiner<A> combiner;

  public ArrTable(ArrCombiner<A> combiner) {
    super();
    this.combiner = combiner;
  }

  public ArrCombiner<A> getCombiner() {
    return this.combiner;
  }

  @Override
  protected boolean checkIfPartitionAddable(
    ArrPartition<A> p) {
    return true;
  }

  @Override
  protected PartitionStatus combinePartition(
    ArrPartition<A> curP, ArrPartition<A> newP) {
    // Try to combine on current partition
    return combiner.combine(curP, newP);
  }
}
