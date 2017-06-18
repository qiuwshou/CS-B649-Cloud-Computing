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

public class MsgTable<I extends VertexID, M extends MsgVal>
  extends TupleTable {

  public MsgTable(Class<I> iClass,
    Class<M> mClass, int byteArrSize,
    int partitionSeed, ResourcePool pool) {
    super(new Class[] { iClass, mClass },
      byteArrSize, partitionSeed, pool);
  }

  public MsgTable(Class<I> iClass,
    Class<M> mClass, int partitionSeed,
    ResourcePool pool) {
    super(new Class[] { iClass, mClass },
      partitionSeed, pool);
  }

  public boolean addMsg(I vertexID, M msgVal) {
    return addTuple(new StructObject[] {
      vertexID, msgVal });
  }

  public int getPartitionID(StructObject[] tuple) {
    return getPartitionID((I) tuple[0]);
  }

  public int getPartitionID(I vertexID) {
    return Math.abs(vertexID.hashCode()
      % this.getPartitionSeed());
  }
}
