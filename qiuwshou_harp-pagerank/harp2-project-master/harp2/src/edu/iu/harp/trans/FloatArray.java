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

package edu.iu.harp.trans;

import edu.iu.harp.trans.Array;

public class FloatArray extends Array<float[]> {

  public FloatArray(float[] arr, int start,
    int size) {
    super(arr, start, size);
    if (this.array == null) {
      start = -1;
      size = -1;
    } else if (this.start >= this.array.length) {
      this.start = this.array.length;
    } else if (this.size >= this.array.length) {
      this.size = this.array.length;
    }
  }
}
