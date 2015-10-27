package com.facebook.hbase.caches;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

public class CopyOnWriteSynchronizedCache extends LocationCache {

  AtomicReference<TreeMap<byte[], String>>
      currentMap =
      new AtomicReference<>(new TreeMap<byte[], String>(Bytes.BYTES_COMPARATOR));

  @Override
  public synchronized void add(byte[] key, String value) {
    TreeMap<byte[], String> m = currentMap.get();
    TreeMap<byte[], String> newMap = new TreeMap<>(m);

    newMap.put(key, value);
    currentMap.set(newMap);
  }

  public synchronized void addAll(Map<byte[], String> map) {
    TreeMap<byte[], String> m = currentMap.get();
    TreeMap<byte[], String> newMap = new TreeMap<>(m);

    for (Map.Entry<byte[], String> e : map.entrySet()) {
      newMap.put(e.getKey(), e.getValue());
    }
    currentMap.set(newMap);
  }

  @Override
  public String get(byte[] lookingFor) {
    Map.Entry<byte[], String> e = currentMap.get().floorEntry(lookingFor);
    if (e != null) {
      return e.getValue();
    }
    return "DEFAULT";
  }

  @Override
  public synchronized void remove(byte[] key) {
    TreeMap<byte[], String> m = currentMap.get();
    TreeMap<byte[], String> newMap = new TreeMap<>(m);

    Map.Entry<byte[], String> e = newMap.floorEntry(key);
    if (e != null) {
      newMap.remove(e.getKey());
    }
    currentMap.set(newMap);
  }
}
