package com.facebook.hbase.caches;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.TreeMap;

public class CopyOnWriteVolatile extends LocationCache {

  volatile TreeMap<byte[], String> currentMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  @Override
  public synchronized void add(byte[] key, String value) {
    TreeMap<byte[], String> m = currentMap;
    TreeMap<byte[], String> newMap = new TreeMap<>(m);

    newMap.put(key, value);
    currentMap = newMap;
  }

  public synchronized void addAll(Map<byte[], String> map) {
    TreeMap<byte[], String> m = currentMap;
    TreeMap<byte[], String> newMap = new TreeMap<>(m);

    for (Map.Entry<byte[], String> e : map.entrySet()) {
      newMap.put(e.getKey(), e.getValue());
    }
    currentMap = newMap;
  }

  @Override
  public String get(byte[] lookingFor) {
    Map.Entry<byte[], String> e = currentMap.floorEntry(lookingFor);
    if (e != null) {
      return e.getValue();
    }
    return "DEFAULT";
  }

  @Override
  public synchronized void remove(byte[] key) {
    TreeMap<byte[], String> m = currentMap;
    TreeMap<byte[], String> newMap = new TreeMap<>(m);

    Map.Entry<byte[], String> e = newMap.floorEntry(key);
    if (e != null) {
      newMap.remove(e.getKey());
    }
    currentMap = newMap;
  }
}
