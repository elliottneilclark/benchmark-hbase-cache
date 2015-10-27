package com.facebook.hbase;


import org.apache.hadoop.hbase.util.Bytes;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

@State(Scope.Group)
public abstract class LocationCache {
  private static final int SEED = 42;

  abstract void add(byte[] key, String value);

  abstract String get(byte[] lookingFor);

  abstract void remove(byte[] key);

  void addAll(Map<byte[], String> map) {
    for (Map.Entry<byte[], String> e : map.entrySet()) {
      add(e.getKey(), e.getValue());
    }
  }

  @Param({"10","1000"})
  public int startingKeys;

  @Setup
  public void setup() {
    Random r = new Random(SEED);
    Map<byte[], String> m = new TreeMap<byte[], String>(Bytes.BYTES_COMPARATOR);

    for (int i = 0; i < startingKeys; i++) {
      long key = Math.abs(r.nextLong());
      String value = String.valueOf(r.nextLong());

      m.put(Bytes.toBytes(key), value);
    }
    addAll(m);
  }
}
