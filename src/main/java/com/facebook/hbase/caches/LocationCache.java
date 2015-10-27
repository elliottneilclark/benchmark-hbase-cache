package com.facebook.hbase.caches;


import org.apache.hadoop.hbase.util.Bytes;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

@State(Scope.Group)
public abstract class LocationCache {
  private static final int SEED = 42;
  private static final int STARTING_KEYS = 1000;

  public abstract void add(byte[] key, String value);

  public abstract String get(byte[] lookingFor);

  public abstract void remove(byte[] key);

  public void addAll(Map<byte[], String> map) {
    for (Map.Entry<byte[], String> e : map.entrySet()) {
      add(e.getKey(), e.getValue());
    }
  }

  @Setup
  public void setup() {
    Random r = new Random(SEED);
    Map<byte[], String> m = new TreeMap<>(Bytes.BYTES_COMPARATOR);

    for (int i = 0; i < STARTING_KEYS; i++) {
      long l;
      do {
        l = r.nextLong();
      } while (l == Long.MIN_VALUE);

      String value = String.valueOf(r.nextLong());

      m.put(Bytes.toBytes(l), value);
    }
    addAll(m);
  }
}
