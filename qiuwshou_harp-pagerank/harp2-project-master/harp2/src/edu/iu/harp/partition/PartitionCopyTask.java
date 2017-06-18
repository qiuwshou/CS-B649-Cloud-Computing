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

import org.apache.log4j.Logger;

import edu.iu.harp.compute.ComputeTask;
import edu.iu.harp.io.Data;
import edu.iu.harp.resource.ResourcePool;

public class PartitionCopyTask extends
  ComputeTask {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(PartitionCopyTask.class);

  private final ResourcePool resourcePool;

  public PartitionCopyTask(ResourcePool pool) {
    resourcePool = pool;
  }

  @Override
  public Data run(Object obj) throws Exception {
    Data data = null;
    Data newData = null;
    try {
      data = (Data) obj;
      data.encodeHead(resourcePool);
      data.encodeBody(resourcePool);
      newData =
        new Data(data.getHeadArray(),
          data.getBodyArray());
      data = null;
      newData.decodeHeadArray(resourcePool);
      newData.releaseHeadArray(resourcePool);
      newData.decodeBodyArray(resourcePool);
      newData.releaseBodyArray(resourcePool);
    } catch (Exception e) {
      LOG.error("Fail to copy.", e);
      data = null;
      newData = null;
    }
    return newData;
  }
}
