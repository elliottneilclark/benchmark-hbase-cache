package com.facebook.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class Benchmark {


  @State(Scope.Group)
  public static class LockingCache {
    protected final LocationCache c = new ReadWriteLockingCache();

    @Setup
    public void setup() {
      Random r = new Random(42);

      for ( int i = 0; i < 1000; i++) {
        long key = Math.abs(r.nextLong());
        String value = String.valueOf(r.nextLong());

        c.add(Bytes.toBytes(key), value);
      }
    }

  }


  @State(Scope.Group)
  public static class ConcurrentCache {
    final LocationCache c = new ReadWriteLockingCache();

    @Setup
    public void setup() {
      Random r = new Random(42);

      for ( int i = 0; i < 1000; i++) {
        long key = Math.abs(r.nextLong());
        String value = String.valueOf(r.nextLong());

        c.add(Bytes.toBytes(key), value);
      }
    }
  }

  @org.openjdk.jmh.annotations.Benchmark
  @Group("locking")
  @GroupThreads(3)
  public String testGetLocking(LockingCache cacheHolder) {
    byte[] key = Bytes.toBytes(Math.abs(ThreadLocalRandom.current().nextLong()));
    return cacheHolder.c.get(key);
  }

  @org.openjdk.jmh.annotations.Benchmark
  @Group("locking")
  @GroupThreads(1)
  public void testPutLocking(LockingCache cacheHolder) {
    byte[] key = Bytes.toBytes(Math.abs(ThreadLocalRandom.current().nextLong()));
    String value = String.valueOf(ThreadLocalRandom.current().nextLong());
    cacheHolder.c.add(key, value);
  }


  @org.openjdk.jmh.annotations.Benchmark
  @Group("baseline")
  @GroupThreads(3)
  public String testGetConcurrent(ConcurrentCache cacheHolder) {
    byte[] key = Bytes.toBytes(Math.abs(ThreadLocalRandom.current().nextLong()));
    return cacheHolder.c.get(key);
  }

  @org.openjdk.jmh.annotations.Benchmark
  @Group("baseline")
  @GroupThreads(1)
  public void testPutConcurrent(ConcurrentCache cacheHolder) {
    byte[] key = Bytes.toBytes(Math.abs(ThreadLocalRandom.current().nextLong()));
    String value = String.valueOf(ThreadLocalRandom.current().nextLong());
    cacheHolder.c.add(key, value);
  }

  @org.openjdk.jmh.annotations.Benchmark
  @Group("baseline")
  @GroupThreads(1)
  public void testRemoveConcurrent(ConcurrentCache cacheHolder) {
    byte[] key = Bytes.toBytes(Math.abs(ThreadLocalRandom.current().nextLong()));
    String value = String.valueOf(ThreadLocalRandom.current().nextLong());
    cacheHolder.c.add(key, value);
  }
}
