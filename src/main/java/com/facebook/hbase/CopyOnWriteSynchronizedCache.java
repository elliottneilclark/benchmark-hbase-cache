package com.facebook.hbase;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

public class CopyOnWriteSynchronizedCache extends LocationCache {

  AtomicReference<TreeMap<byte[], String>>
      currentMap =
      new AtomicReference<TreeMap<byte[], String>>(new TreeMap<byte[], String>(Bytes.BYTES_COMPARATOR));

  @Override
  synchronized void add(byte[] key, String value) {
    TreeMap<byte[], String> m = currentMap.get();
    TreeMap<byte[], String> newMap = new TreeMap<byte[], String>(m);

    newMap.put(key, value);
    currentMap.set(newMap);
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
  synchronized void remove(byte[] key) {
    TreeMap<byte[], String> m = currentMap.get();
    TreeMap<byte[], String> newMap = new TreeMap<byte[], String>(m);

    Map.Entry<byte[], String> e = newMap.floorEntry(key);
    if (e != null) {
      newMap.remove(e.getKey());
    }
    currentMap.set(newMap);
  }
}