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

package edu.iu.harp.primitivekv;

import edu.iu.harp.io.Constants;
import edu.iu.harp.keyval.KeyValStatus;
import edu.iu.harp.partition.StructPartition;
import edu.iu.harp.utils.Long2ObjValHeldHashMap;

public abstract class AbsLong2ObjKVPartition<V>
  extends StructPartition {

  private Long2ObjValHeldHashMap<V> kvMap;

  public AbsLong2ObjKVPartition() {
    super();
    setPartitionID(Constants.UNKNOWN_PARTITION_ID);
    kvMap = null;
  }

  public void initialize(int partitionID,
    int expectedKVCount) {
    this.setPartitionID(partitionID);
    if (this.kvMap == null) {
      createKVMap(expectedKVCount);
    }
  }

  protected void createKVMap(int size) {
    this.kvMap = new Long2ObjValHeldHashMap<V>(size);
    this.kvMap.defaultReturnValue(null);
  }

  public abstract KeyValStatus putKeyVal(long key,
    V val);

  public V getVal(long key) {
    return this.kvMap.get(key);
  }

  public Long2ObjValHeldHashMap<V> getKVMap() {
    return kvMap;
  }

  public int size() {
    return this.kvMap.size();
  }

  public boolean isEmpty() {
    return this.kvMap.isEmpty();
  }
}
