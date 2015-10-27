package com.facebook.hbase;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by elliott on 10/26/15.
 */
public class ConcurrentCache implements LocationCache {
  private ConcurrentSkipListMap<byte[], String>
      locations =
      new ConcurrentSkipListMap<byte[], String>(Bytes.BYTES_COMPARATOR);

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
