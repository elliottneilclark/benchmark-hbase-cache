package com.facebook.hbase;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReadWriteLockingCache extends LocationCache {
  private TreeMap<byte[], String> locations = new TreeMap<byte[], String>(Bytes.BYTES_COMPARATOR);
  ReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);

  @Override
  public void add(byte[] key, String value) {
    Lock l = readWriteLock.writeLock();
    l.lock();
    try {
      locations.put(key, value);
    } finally {
      l.unlock();
    }

  }

  @Override
  public String get(byte[] lookingFor) {
    Lock l = readWriteLock.readLock();
    l.lock();
    try {
      Map.Entry<byte[], String> e = locations.floorEntry(lookingFor);
      if (e != null) {
        return e.getValue();
      }
      return "DEFAULT";
    } finally {
      l.unlock();
    }
  }

  @Override
  public void remove(byte[] key) {
    Lock l = readWriteLock.writeLock();
    l.lock();
    try {
      Map.Entry<byte[], String> e = locations.floorEntry(key);
      if (e != null) {
        locations.remove(e.getKey());
      }
    } finally {
      l.unlock();
    }
  }
}
