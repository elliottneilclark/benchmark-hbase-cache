package com.facebook.hbase;

/**
 * Created by elliott on 10/26/15.
 */
public interface LocationCache {
  void add(byte[] key, String value);

  String get(byte[] lookingFor);

  void remove(byte[] key);
}
