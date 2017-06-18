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

package edu.iu.harp.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntVertexID extends VertexID {
  private int vertexID;

  public IntVertexID() {
  }

  public IntVertexID(int id) {
    vertexID = id;
  }

  public void setVertexID(int id) {
    this.vertexID = id;
  }

  public int getVertexID() {
    return vertexID;
  }

  @Override
  public int getSizeInBytes() {
    return 4;
  }

  @Override
  public void write(DataOutput out)
    throws IOException {
    out.writeInt(vertexID);
  }

  @Override
  public void read(DataInput in)
    throws IOException {
    vertexID = in.readInt();
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof IntVertexID) {
      IntVertexID key = (IntVertexID) object;
      return vertexID == key.getVertexID();
    } else {
      return false;
    }
  }

  public int hashCode() {
    return vertexID;
  }

  @Override
  public void clear() {
  }
}