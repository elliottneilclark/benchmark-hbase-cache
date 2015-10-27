package com.facebook.hbase.caches;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConcurrentCache extends LocationCache {
  private ConcurrentSkipListMap<byte[], String>
      locations =
      new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);

  public void add(byte[] key, String value) {
    locations.put(key, value);
  }

  @Override
  public String get(byte[] lookingFor) {
    Map.Entry<byte[], String> e = locations.floorEntry(lookingFor);
    if (e != null) {
      return e.getValue();
    }
    return "DEFAULT";
  }

  @Override
  public void remove(byte[] key) {
    Map.Entry<byte[], String> e = locations.floorEntry(key);
    if (e != null) {
      locations.remove(e.getKey());
    }
  }
}
