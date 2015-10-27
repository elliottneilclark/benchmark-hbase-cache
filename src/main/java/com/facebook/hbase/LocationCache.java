package com.facebook.hbase;


import org.apache.hadoop.hbase.util.Bytes;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Random;

@State(Scope.Group)
public abstract class LocationCache {
  abstract void add(byte[] key, String value);

  abstract String get(byte[] lookingFor);

  abstract void remove(byte[] key);

  private static final int SEED = 42;

  private static long STARTING_ROWS = 1000L;

  @Setup
  public void setup() {
    Random r = new Random(SEED);

    for ( int i = 0; i < STARTING_ROWS; i++) {
      long key = Math.abs(r.nextLong());
      String value = String.valueOf(r.nextLong());

      add(Bytes.toBytes(key), value);
    }
  }
}
