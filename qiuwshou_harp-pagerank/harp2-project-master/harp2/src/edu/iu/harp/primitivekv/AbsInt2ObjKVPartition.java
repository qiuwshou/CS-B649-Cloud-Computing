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
import edu.iu.harp.utils.Int2ObjValHeldHashMap;

public abstract class AbsInt2ObjKVPartition<V>
  extends StructPartition {

  private Int2ObjValHeldHashMap<V> kvMap;

  public AbsInt2ObjKVPartition() {
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
    // If a map exists, this object must be
    // released and cleared before
  }

  protected void createKVMap(int size) {
    this.kvMap =
      new Int2ObjValHeldHashMap<V>(size);
    // default return value and default value on
    // the value array both are null
    this.kvMap.defaultReturnValue(null);
  }

  public abstract KeyValStatus putKeyVal(int key,
    V val);

  public V getVal(int key) {
    return this.kvMap.get(key);
  }

  public V removeVal(int key) {
    return this.kvMap.remove(key);
  }

  public Int2ObjValHeldHashMap<V> getKVMap() {
    return kvMap;
  }

  public int size() {
    return this.kvMap.size();
  }

  public boolean isEmpty() {
    return this.kvMap.isEmpty();
  }
}
