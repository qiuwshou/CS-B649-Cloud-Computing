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

package edu.iu.harp.graph;

import edu.iu.harp.partition.TupleTable;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.StructObject;

public abstract class EdgeTable<I extends VertexID, E extends EdgeVal>
  extends TupleTable {

  public EdgeTable(Class<I> iClass,
    Class<E> eClass, int byteArrSize,
    int partitionSeed, ResourcePool pool) {
    super(new Class[] { iClass, eClass, iClass },
      byteArrSize, partitionSeed, pool);
  }

  public EdgeTable(Class<I> iClass,
    Class<E> eClass, int partitionSeed,
    ResourcePool pool) {
    super(new Class[] { iClass, eClass, iClass },
      partitionSeed, pool);
  }

  public boolean addEdge(I sourceID, E edgeVal,
    I targetID) {
    return addTuple(new StructObject[] {
      sourceID, edgeVal, targetID });
  }

  public int getPartitionID(StructObject[] tuple) {
    return getPartitionID((I) tuple[0],
      (I) tuple[2]);
  }

  public abstract int getPartitionID(I sourceID,
    I targetID);
}
