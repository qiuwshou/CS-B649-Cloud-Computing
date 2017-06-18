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

import edu.iu.harp.resource.ResourcePool;

public class OutEdgeTable<I extends VertexID, E extends EdgeVal>
  extends EdgeTable<I, E> {

  public OutEdgeTable(Class<I> iClass,
    Class<E> eClass, int byteArrSize,
    int partitionSeed, ResourcePool pool) {
    super(iClass, eClass, byteArrSize,
      partitionSeed, pool);
  }

  public OutEdgeTable(Class<I> iClass,
    Class<E> eClass, int partitionSeed,
    ResourcePool pool) {
    super(iClass, eClass, partitionSeed, pool);
  }

  @Override
  public int
    getPartitionID(I sourceID, I targetID) {
    return Math.abs(sourceID.hashCode()
      % this.getPartitionSeed());
  }
}