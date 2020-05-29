package com.nus.cool.core.io.cache;


import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class CacheManager {

  private static MemoryStore memoryStore;

  private static DiskStore diskStore;

  public CacheManager(String cacheRoot, int memoryCacheSize, int diskCacheSize,
      double entryCacheLimit) {
    memoryStore = new MemoryStore(memoryCacheSize, entryCacheLimit);
    diskStore = new DiskStore(cacheRoot, diskCacheSize, entryCacheLimit);
  }

  public void put(CacheKey cacheKey, BitSet bitSet, String storageLevel) throws IOException {
    checkNotNull(storageLevel);
    if ("MEMORY_ONLY".equals(storageLevel)) {
      memoryStore.put(cacheKey, bitSet);
    } else if ("DISK_ONLY".equals(storageLevel)) {
      diskStore.put(cacheKey, bitSet);
    } else {
      throw new IllegalArgumentException("Illegal storageLevel: " + storageLevel);
    }
  }

  public Map<Integer, BitSet> load(List<CacheKey> cacheKeys, String storageLevel)
      throws IOException {
    if ("MEMORY_ONLY".equals(storageLevel)) {
      return memoryStore.load(cacheKeys);
    } else if ("DISK_ONLY".equals(storageLevel)) {
      return diskStore.load(cacheKeys);
    } else {
      throw new IllegalArgumentException("Illegal storageLevel: " + storageLevel);
    }
  }
}
