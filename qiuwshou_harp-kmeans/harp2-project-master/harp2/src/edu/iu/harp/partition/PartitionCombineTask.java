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

package edu.iu.harp.partition;

import java.util.LinkedList;

import edu.iu.harp.compute.ComputeTask;
import edu.iu.harp.resource.ResourcePool;

public class PartitionCombineTask extends
  ComputeTask {

  private final ResourcePool resourcePool;
  private final Table<? extends Partition> table;
  private final TableType tableType;
  private final PartitionType partitionType;

  public PartitionCombineTask(
    Table<? extends Partition> table,
    ResourcePool pool, TableType tableType,
    PartitionType partitionType) {
    this.table = table;
    resourcePool = pool;
    this.tableType = tableType;
    this.partitionType = partitionType;
  }

  @Override
  public Object run(Object input)
    throws Exception {
    LinkedList<Partition> partitionList =
      (LinkedList<Partition>) input;
    for (Partition partition : partitionList) {
      Partition curPartition =
        table.getPartition(partition
          .getPartitionID());
      PartitionUtils.combinePartitionToTable(
        curPartition, partition, partitionType,
        table, tableType, resourcePool);
    }
    return null;
  }
}
