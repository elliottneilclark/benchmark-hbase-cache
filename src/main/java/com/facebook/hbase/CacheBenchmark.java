package com.facebook.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;

import java.util.concurrent.ThreadLocalRandom;

public class CacheBenchmark {



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


  /****** Copy On Write ****************/
  @org.openjdk.jmh.annotations.Benchmark
  @Group("cow")
  @GroupThreads(6)
  public String testGet(CopyOnWriteCache cache) {
    return doGet(cache);
  }

  @org.openjdk.jmh.annotations.Benchmark
  @Group("cow")
  @GroupThreads(1)
  public void testPut(CopyOnWriteCache cache) {
    doPut(cache);
  }

  @org.openjdk.jmh.annotations.Benchmark
  @Group("cow")
  @GroupThreads(1)
  public void testRemove(CopyOnWriteCache cache) {
    doDelete(cache);
  }

  /****** LOCKING ****************/
  @org.openjdk.jmh.annotations.Benchmark
  @Group("locking")
  @GroupThreads(6)
  public String testGet(ReadWriteLockingCache cache) {
    return doGet(cache);
  }

  @org.openjdk.jmh.annotations.Benchmark
  @Group("locking")
  @GroupThreads(1)
  public void testPut(ReadWriteLockingCache cache) {
    doPut(cache);
  }

  @org.openjdk.jmh.annotations.Benchmark
  @Group("locking")
  @GroupThreads(1)
  public void testRemove(ReadWriteLockingCache cache) {
    doDelete(cache);
  }


  /****** BASELINE ********************/
  @org.openjdk.jmh.annotations.Benchmark
  @Group("baseline")
  @GroupThreads(6)
  public String testGet(ConcurrentCache cache) {
    return doGet(cache);
  }

  @org.openjdk.jmh.annotations.Benchmark
  @Group("baseline")
  @GroupThreads(1)
  public void testPut(ConcurrentCache cache) {
    doPut(cache);
  }

  @org.openjdk.jmh.annotations.Benchmark
  @Group("baseline")
  @GroupThreads(1)
  public void testRemove(ConcurrentCache cache) {
    doDelete(cache);
  }
}
