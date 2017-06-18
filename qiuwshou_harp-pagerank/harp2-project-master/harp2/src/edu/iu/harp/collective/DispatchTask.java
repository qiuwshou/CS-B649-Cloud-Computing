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

package edu.iu.harp.collective;

import org.apache.log4j.Logger;

import edu.iu.harp.compute.ComputeTask;
import edu.iu.harp.io.Data;
import edu.iu.harp.resource.ResourcePool;

class DispatchData {
  Data data;
  int destID;

  DispatchData(Data data, int destID) {
    this.data = data;
    this.destID = destID;
  }
}

public class DispatchTask extends ComputeTask {

  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(DispatchTask.class);

  private final ResourcePool resourcePool;

  public DispatchTask(ResourcePool pool) {
    resourcePool = pool;
  }

  @Override
  public Object run(Object obj) throws Exception {
    DispatchData dispatchData =
      (DispatchData) obj;
    try {
      dispatchData.data.encodeHead(resourcePool);
      dispatchData.data.encodeBody(resourcePool);
    } catch (Exception e) {
      LOG.error("Fail to encode.", e);
    }
    return dispatchData;
  }
}
