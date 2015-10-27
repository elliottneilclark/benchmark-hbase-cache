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

public class CacheBenchmark {

  private static final int SEED = 42;
  private static long STARTING_ROWS = 1000L;

  @State(Scope.Group)
  public static class LockingCache {
    protected final LocationCache c = new ReadWriteLockingCache();

    @Setup
    public void setup() {
      Random r = new Random(SEED);

      for ( int i = 0; i < STARTING_ROWS; i++) {
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

      for ( int i = 0; i < STARTING_ROWS; i++) {
        long key = Math.abs(r.nextLong());
        String value = String.valueOf(r.nextLong());

        c.add(Bytes.toBytes(key), value);
      }
    }
  }

  private String doGet(LocationCache c) {
    byte[] key = Bytes.toBytes(Math.abs(ThreadLocalRandom.current().nextLong()));
    return c.get(key);
  }

  private void doPut(LocationCache cache) {
    byte[] key = Bytes.toBytes(Math.abs(ThreadLocalRandom.current().nextLong()));
    String value = String.valueOf(ThreadLocalRandom.current().nextLong());
    cache.add(key, value);
  }

  private void doDelete(LocationCache cache) {
    byte[] key = Bytes.toBytes(Math.abs(ThreadLocalRandom.current().nextLong()));
    cache.remove(key);
  }

  /****** LOCKING ****************/
  @org.openjdk.jmh.annotations.Benchmark
  @Group("locking")
  @GroupThreads(6)
  public String testGetLocking(LockingCache cacheHolder) {
    return doGet(cacheHolder.c);
  }

  @org.openjdk.jmh.annotations.Benchmark
  @Group("locking")
  @GroupThreads(1)
  public void testPutLocking(LockingCache cacheHolder) {
    doPut(cacheHolder.c);
  }

  @org.openjdk.jmh.annotations.Benchmark
  @Group("locking")
  @GroupThreads(1)
  public void testRemoveLocking(LockingCache cacheHolder) {
    doDelete(cacheHolder.c);
  }


  /****** BASELINE ********************/
  @org.openjdk.jmh.annotations.Benchmark
  @Group("baseline")
  @GroupThreads(6)
  public String testGetConcurrent(ConcurrentCache cacheHolder) {
    return doGet(cacheHolder.c);
  }

  @org.openjdk.jmh.annotations.Benchmark
  @Group("baseline")
  @GroupThreads(1)
  public void testPutConcurrent(ConcurrentCache cacheHolder) {
    doPut(cacheHolder.c);
  }

  @org.openjdk.jmh.annotations.Benchmark
  @Group("baseline")
  @GroupThreads(1)
  public void testRemoveConcurrent(ConcurrentCache cacheHolder) {
    doDelete(cacheHolder.c);
  }
}
