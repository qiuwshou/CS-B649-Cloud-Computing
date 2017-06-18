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
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Arrays;

/**
 * This map tries to reserve the value object
 * allocation and allows the spaces allocated can
 * be reused after clean the whole map. Notice
 * that if remove or remove in iterator is used,
 * the space will still be deallocated.
 * 
 * @author zhangbj
 *
 * @param <V>
 */
public class Long2ObjValHeldHashMap<V> extends
  Long2ObjectOpenHashMap<V> {

  /** Follow the settings in the parent class */
  private static final long serialVersionUID = 0L;

  public Long2ObjValHeldHashMap(int expected) {
    super(expected);
  }

  private V
    insertIfAbsent(final long k, final V v) {
    int pos;
    if (((k) == (0))) {
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
      long curr;
      final long[] key = this.key;
      // The starting point.
      if (!((curr =
        key[pos =
          (int) it.unimi.dsi.fastutil.HashCommon
            .mix((k)) & mask]) == (0))) {
        if (((curr) == (k))) {
          final V oldValue = value[pos];
          if (oldValue == null) {
            value[pos] = v;
          }
          return oldValue;
        }
        while (!((curr =
          key[pos = (pos + 1) & mask]) == (0))) {
          if (((curr) == (k))) {
            final V oldValue = value[pos];
            if (oldValue == null) {
              value[pos] = v;
            }
            return oldValue;
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
    // In Long2ObjectOpenHashMap,
    // checkTable() is empty
    // if ( ASSERTS ) checkTable();
    return oldValue;
  }

  public V putIfAbsent(final long k, final V v) {
    return insertIfAbsent(k, v);
  }

  public void clearKeys() {
    if (size == 0)
      return;
    size = 0;
    containsNullKey = false;
    Arrays.fill(key, (0));
    // Arrays.fill( value, null );
  }
}
