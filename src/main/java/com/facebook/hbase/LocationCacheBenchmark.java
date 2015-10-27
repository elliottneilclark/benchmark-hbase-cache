package com.facebook.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Measurement(iterations = 10, time = 30, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
@Fork(value = 5, jvmArgsPrepend = "-server")
public class LocationCacheBenchmark {

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

  /******
   * Copy On Write Synchronized
   ****************/
  @Benchmark
  @Group("cow_synchronized")
  @GroupThreads(14)
  public String testGet(CopyOnWriteSynchronizedCache cache) {
    return doGet(cache);
  }

  @Benchmark
  @Group("cow_synchronized")
  @GroupThreads(1)
  public void testPut(CopyOnWriteSynchronizedCache cache) {
    doPut(cache);
  }

  @Benchmark
  @Group("cow_synchronized")
  @GroupThreads(1)
  public void testRemove(CopyOnWriteSynchronizedCache cache) {
    doDelete(cache);
  }

  /******
   * Copy On Write
   ****************/
  @Benchmark
  @Group("cow")
  @GroupThreads(14)
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

  /******
   * locking
   ****************/
  @Benchmark
  @Group("locking")
  @GroupThreads(14)
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


  /******
   * BASELINE
   ********************/
  @Benchmark
  @Group("baseline")
  @GroupThreads(14)
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