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

package edu.iu.harp.test;

import edu.iu.harp.graph.EdgeTable;
import edu.iu.harp.graph.InEdgeTable;
import edu.iu.harp.graph.LongVertexID;
import edu.iu.harp.graph.NullEdgeVal;
import edu.iu.harp.partition.TuplePartition;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.StructObject;

public class GraphTest {

  public static void main(String[] args) {
    ResourcePool pool = new ResourcePool();
    EdgeTable<LongVertexID, NullEdgeVal> edgeTable =
      new InEdgeTable<LongVertexID, NullEdgeVal>(
        LongVertexID.class, NullEdgeVal.class,
        100, pool);
    edgeTable.addEdge(new LongVertexID(1),
      new NullEdgeVal(), new LongVertexID(2));
    edgeTable.addEdge(new LongVertexID(3),
      new NullEdgeVal(), new LongVertexID(4));
    edgeTable.addEdge(new LongVertexID(9),
      new NullEdgeVal(), new LongVertexID(10));
    TuplePartition partition =
      edgeTable.getPartitions().iterator().next();
    System.out.println("Partition: "
      + partition.getPartitionID() + " "
      + partition.getByteArrays().size());
    while (partition.readTuple()) {
      StructObject[] tuple = partition.getTuple();
      System.out
        .println("SOURCE VTX ID: "
          + ((LongVertexID) tuple[0])
            .getVertexID()
          + " TARGET VTX ID: "
          + ((LongVertexID) tuple[2])
            .getVertexID());
    }
    System.out.println("Back to default");
    partition.defaultReadPos();
    while (partition.readTuple()) {
      StructObject[] tuple = partition.getTuple();
      LongVertexID vtxID =
        (LongVertexID) tuple[0];
      if (vtxID.getVertexID() == 3) {
        partition.removeTuple();
        break;
      }
    }
    edgeTable.addEdge(new LongVertexID(11),
      new NullEdgeVal(), new LongVertexID(12));
    System.out.println("Back to default");
    partition.defaultReadPos();
    System.out.println("Partition: "
      + partition.getPartitionID() + " "
      + partition.getByteArrays().size());
    while (partition.readTuple()) {
      StructObject[] tuple = partition.getTuple();
      System.out
        .println("SOURCE VTX ID: "
          + ((LongVertexID) tuple[0])
            .getVertexID()
          + " TARGET VTX ID: "
          + ((LongVertexID) tuple[2])
            .getVertexID());

    }
  }
}
