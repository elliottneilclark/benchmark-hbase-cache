package com.facebook.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
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

  /****** Copy On Write synchronized ****************/
  @Benchmark
  @Group("cow.synchronized")
  @GroupThreads(6)
  public String testGet(CopyOnWriteSynchronizedCache cache) {
    return doGet(cache);
  }

  @Benchmark
  @Group("cow.synchronized")
  @GroupThreads(1)
  public void testPut(CopyOnWriteSynchronizedCache cache) {
    doPut(cache);
  }

  @Benchmark
  @Group("cow.synchronized")
  @GroupThreads(1)
  public void testRemove(CopyOnWriteSynchronizedCache cache) {
    doDelete(cache);
  }

  /****** Copy On Write ****************/
  @Benchmark
  @Group("cow")
  @GroupThreads(6)
  public String testGet(CopyOnWriteCache cache) {
    return doGet(cache);
  }

  @Benchmark
  @Group("cow")
  @GroupThreads(1)
  public void testPut(CopyOnWriteCache cache) {
    doPut(cache);
  }

  @Benchmark
  @Group("cow")
  @GroupThreads(1)
  public void testRemove(CopyOnWriteCache cache) {
    doDelete(cache);
  }

  /****** LOCKING ****************/
  @Benchmark
  @Group("locking")
  @GroupThreads(6)
  public String testGet(ReadWriteLockingCache cache) {
    return doGet(cache);
  }

  @Benchmark
  @Group("locking")
  @GroupThreads(1)
  public void testPut(ReadWriteLockingCache cache) {
    doPut(cache);
  }

  @Benchmark
  @Group("locking")
  @GroupThreads(1)
  public void testRemove(ReadWriteLockingCache cache) {
    doDelete(cache);
  }


  /****** BASELINE ********************/
  @Benchmark
  @Group("baseline")
  @GroupThreads(6)
  public String testGet(ConcurrentCache cache) {
    return doGet(cache);
  }

  @Benchmark
  @Group("baseline")
  @GroupThreads(1)
  public void testPut(ConcurrentCache cache) {
    doPut(cache);
  }

  @Benchmark
  @Group("baseline")
  @GroupThreads(1)
  public void testRemove(ConcurrentCache cache) {
    doDelete(cache);
  }
}
