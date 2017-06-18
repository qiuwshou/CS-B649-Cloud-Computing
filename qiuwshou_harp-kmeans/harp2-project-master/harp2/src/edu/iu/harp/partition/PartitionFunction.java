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

/**
 * Convert array from an old type to a new type.
 * 
 * @author zhangbj
 *
 * @param <OA>
 *          Original array type
 * @param <NA>
 *          New array type
 */
public abstract class PartitionFunction<P extends Partition> {
  public abstract void apply(P partition)
    throws Exception;
}
