package com.nus.cool.core.io.cache;


import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class CacheManager {

  private static DiskStore diskStore;

  public CacheManager(String cacheRoot, int diskCacheSize, double entryCacheLimit) {
    diskStore = new DiskStore(cacheRoot, diskCacheSize, entryCacheLimit);
  }

  public void put(CacheKey cacheKey, BitSet bitSet) throws IOException {
    diskStore.put(cacheKey, bitSet);
  }

  public Map<Integer, BitSet> load(List<CacheKey> cacheKeys) throws IOException {
    return diskStore.load(cacheKeys);
  }
}
