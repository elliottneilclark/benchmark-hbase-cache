package com.facebook.hbase;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

public class CopyOnWriteCache extends LocationCache {

  AtomicReference<TreeMap<byte[], String>>
      currentMap
      =
      new AtomicReference<TreeMap<byte[], String>>(new TreeMap<byte[], String>(Bytes.BYTES_COMPARATOR));

  @Override
  void add(byte[] key, String value) {
    while (true) {
      TreeMap<byte[], String> m = currentMap.get();
      TreeMap<byte[], String> newMap = new TreeMap<byte[], String>(m);

      newMap.put(key, value);
      if (currentMap.compareAndSet(m, newMap)) {
        return;
      }
    }
  }

  void addAll(Map<byte[], String> map) {
    while (true) {
      TreeMap<byte[], String> m = currentMap.get();
      TreeMap<byte[], String> newMap = new TreeMap<byte[], String>(m);

      for (Map.Entry<byte[], String> e : map.entrySet()) {
        newMap.put(e.getKey(), e.getValue());
      }
      if (currentMap.compareAndSet(m, newMap)) {
        return;
      }
    }
  }

  @Override
  String get(byte[] lookingFor) {
    Map.Entry<byte[], String> e = currentMap.get().floorEntry(lookingFor);
    if (e != null) {
      return e.getValue();
    }
    return "DEFAULT";
  }

  @Override
  void remove(byte[] key) {
    while (true) {
      TreeMap<byte[], String> m = currentMap.get();
      TreeMap<byte[], String> newMap = new TreeMap<byte[], String>(m);

      Map.Entry<byte[], String> e = newMap.floorEntry(key);
      if (e != null) {
        newMap.remove(e.getKey());
      }
      if (currentMap.compareAndSet(m, newMap)) {
        return;
      }
    }
  }
}
