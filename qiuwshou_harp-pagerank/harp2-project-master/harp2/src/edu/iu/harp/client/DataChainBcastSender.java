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

package edu.iu.harp.client;

import org.apache.log4j.Logger;

import edu.iu.harp.io.Constants;
import edu.iu.harp.io.Data;
import edu.iu.harp.io.Serializer;
import edu.iu.harp.resource.ResourcePool;
import edu.iu.harp.trans.ByteArray;
import edu.iu.harp.worker.Workers;

/**
 * We don't allow the worker broadcasts to itself.
 */
public class DataChainBcastSender extends
  DataSender {
  /** Class logger */
  private static final Logger LOG = Logger
    .getLogger(DataChainBcastSender.class);

  public DataChainBcastSender(Data data,
    Workers workers, ResourcePool pool,
    byte command) {
    super(data, getDestID(workers.getSelfID(),
      workers.getNextID()), workers, pool,
      command);
  }

  private static int getDestID(int selfID,
    int nextID) {
    if (selfID == nextID) {
      return Constants.UNKNOWN_WORKER_ID;
    } else {
      return nextID;
    }
  }

  @Override
  protected ByteArray getOPByteArray(
    int headArrSize, int bodyArrSize) {
    byte[] opBytes =
      getResourcePool().getBytes(12);
    try {
      Serializer serializer =
        new Serializer(new ByteArray(opBytes, 0,
          12));
      serializer.writeInt(headArrSize);
      serializer.writeInt(bodyArrSize);
      serializer.writeInt(getWorkers()
        .getSelfID());
    } catch (Exception e) {
      getResourcePool().releaseBytes(opBytes);
      return null;
    }
    return new ByteArray(opBytes, 0, 12);
  }
}
