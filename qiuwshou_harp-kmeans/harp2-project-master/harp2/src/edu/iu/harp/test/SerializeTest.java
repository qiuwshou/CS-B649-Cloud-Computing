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

package edu.iu.harp.test;

import java.util.zip.Deflater;
import java.util.zip.Inflater;

import edu.iu.harp.io.DataUtils;
import edu.iu.harp.io.Deserializer;
import edu.iu.harp.io.Serializer;
import edu.iu.harp.message.Barrier;
import edu.iu.harp.resource.ResourcePool;

public class SerializeTest {

  public static void main(String args[])
    throws Exception {
    ResourcePool pool = new ResourcePool();
    int[] a = { 27, 501, 502 };
    char[] ac = { 'a', 'b', 'c' };
    String s = "wodfdfskjfskf;slfs32409";
    boolean[] ab = { true, false, true };
    short[] as = { 1001, 2003, 4005 };
    long[] al = { 90239, 43980, 43822938 };
    double[] ad =
      { 50000.454, 7845239.58, 24239.23423023 };
    Barrier barrier1 = new Barrier();
    int barrier1Size =
      DataUtils.getStructObjSizeInBytes(barrier1);
    System.out.println("barrier 1 size: "
      + barrier1Size);
    byte[] b =
      new byte[a.length * 4 + ac.length * 2
        + s.length() * 2 + 4 + ab.length * 1
        + as.length * 2 + al.length * 8
        + ad.length * 8 + 2 + 1 + barrier1Size];
    System.out.println("byte array length: "
      + b.length);
    Serializer ds =
      new Serializer(b, 0, b.length);

    for (int i = 0; i < a.length; i++) {
      ds.writeInt(a[i]);
    }
    for (int i = 0; i < ac.length; i++) {
      ds.writeChar(ac[i]);
    }
    ds.writeChars(s);
    for (int i = 0; i < ab.length; i++) {
      ds.writeBoolean(ab[i]);
    }
    for (int i = 0; i < as.length; i++) {
      ds.writeShort(as[i]);
    }
    for (int i = 0; i < al.length; i++) {
      ds.writeLong(al[i]);
    }
    for (int i = 0; i < ad.length; i++) {
      ds.writeDouble(ad[i]);
    }
    ds.writeShort(-2);
    ds.writeByte(-2);
    DataUtils.serializeStructObject(barrier1
      .getClass().getName(), barrier1, ds);
    // Defalte
    try {
      // Compress the bytes
      byte[] output = new byte[b.length * 2];
      // Write real data length
      Serializer dsTmp =
        new Serializer(output, 0, output.length);
      dsTmp.writeInt(b.length);
      System.out.println("original data length: "
        + b.length);
      // Compress the input bytes to output bytes
      Deflater compresser = new Deflater(0, true);
      compresser.setInput(b);
      compresser.finish();
      int compressedDataLength =
        compresser.deflate(output, 4,
          output.length - 4);
      System.out.println("compressedDataLength: "
        + compressedDataLength);
      compresser.end();
      // Decompress the bytes
      Deserializer ddTmp =
        new Deserializer(output, 0, output.length);
      int realDataLength = ddTmp.readInt();
      System.out.println("original data length: "
        + b.length);
      byte[] result = new byte[realDataLength];
      Inflater decompresser = new Inflater(true);
      decompresser.setInput(output, 4,
        compressedDataLength);
      int resultLength =
        decompresser.inflate(result);
      decompresser.end();
      b = result;
    } catch (Exception e) {
      e.printStackTrace();
    }

    Deserializer dd =
      new Deserializer(b, 0, b.length);
    for (int i = 0; i < a.length; i++) {
      System.out.println(dd.readInt());
    }
    for (int i = 0; i < ac.length; i++) {
      System.out.println(dd.readChar());
    }
    System.out.println(dd.readUTF());
    for (int i = 0; i < ab.length; i++) {
      System.out.println(dd.readBoolean());
    }
    for (int i = 0; i < as.length; i++) {
      System.out.println(dd.readShort());
    }
    for (int i = 0; i < al.length; i++) {
      System.out.println(dd.readLong());
    }
    for (int i = 0; i < ad.length; i++) {
      System.out.println(dd.readDouble());
    }
    System.out.println(dd.readUnsignedShort());
    System.out.println(dd.readUnsignedByte());
    Barrier barrier2 =
      (Barrier) DataUtils.deserializeStructObj(
        dd, pool);
    System.out.println(barrier2.getSizeInBytes());
  }
}
