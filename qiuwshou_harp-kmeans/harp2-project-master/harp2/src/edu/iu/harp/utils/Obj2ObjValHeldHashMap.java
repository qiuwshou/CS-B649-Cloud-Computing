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

package edu.iu.harp.utils;

import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.util.Arrays;

/**
 * This map tries to reserve the value object
 * allocation and allows the spaces allocated ca
 * be reused after clean the whole map. Notice
 * that if remove or remove in iterator is used,
 * the space will still be deallocated.
 * 
 * @author zhangbj
 *
 * @param <V>
 */
public class Obj2ObjValHeldHashMap<K, V> extends
  Object2ObjectOpenHashMap<K, V> {

  /** Follow the settings in the parent class */
  private static final long serialVersionUID = 0L;

  public Obj2ObjValHeldHashMap(int expected) {
    super(expected);
  }

  private V insertIfAbsent(final K k, final V v) {
    int pos;
    if (((k) == null)) {
      if (containsNullKey) {
        final V oldValue = value[n];
        if (oldValue == null) {
          value[n] = v;
        }
        return oldValue;
      }
      containsNullKey = true;
      pos = n;
    } else {
      K curr;
      final K[] key = this.key;
      // The starting point.
      if (!((curr =
        key[pos =
          (it.unimi.dsi.fastutil.HashCommon
            .mix((k).hashCode())) & mask]) == null)) {
        if (((curr).equals(k))) {
          final V oldValue = value[pos];
          if (oldValue == null) {
            value[pos] = v;
          }
        }
        while (!((curr =
          key[pos = (pos + 1) & mask]) == null)) {
          if (((curr).equals(k))) {
            final V oldValue = value[pos];
            if (oldValue == null) {
              value[pos] = v;
            }
          }
        }
      }
      key[pos] = k;
    }
    final V oldValue = value[pos];
    if (oldValue == null) {
      value[pos] = v;
    }
    if (size++ >= maxFill)
      rehash(arraySize(size + 1, f));
    // In Object2ObjectOpenHashMap,
    // checkTable() is empty
    // if ( ASSERTS ) checkTable();
    return oldValue;
  }

  /**
   * Put the value if the key's position is
   * associated with a null value. Otherwise
   * return the data structure on the position.
   * 
   * @param k
   * @param v
   * @return
   */
  public V putIfAbsent(final K k, final V v) {
    return insertIfAbsent(k, v);
  }

  public void clearKeys() {
    if (size == 0)
      return;
    size = 0;
    containsNullKey = false;
    Arrays.fill(key, (null));
    // Arrays.fill( value, null );
  }
}
