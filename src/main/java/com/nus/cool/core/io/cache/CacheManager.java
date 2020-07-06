package com.nus.cool.core.io.cache;


import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import lombok.Getter;

public class CacheManager {

  private static MemoryStore memoryStore;

  private static DiskStore diskStore;

  private static Map<CacheEntry, String> toCacheBitsets;

  @Getter
  private static double hitNum;

  @Getter
  private static double totalNum;

  public CacheManager(String cacheRoot, double memoryCacheSize, double diskCacheSize,
      double entryCacheLimit) {
    memoryStore = new MemoryStore(memoryCacheSize, entryCacheLimit);
    diskStore = new DiskStore(cacheRoot, diskCacheSize, entryCacheLimit);
    toCacheBitsets = Maps.newHashMap();
    hitNum = 0;
    totalNum = 0;
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
    checkNotNull(storageLevel);
    // TODO: Need to load from disk cache (MEMORY_AND_DISK)
    totalNum += cacheKeys.size();
    if ("MEMORY_ONLY".equals(storageLevel)) {
      Map<Integer, BitSet> cachedBitsets = memoryStore.load(cacheKeys);
      hitNum += cachedBitsets.size();
      return cachedBitsets;
    } else if ("DISK_ONLY".equals(storageLevel)) {
      Map<Integer, BitSet> cachedBitsets = diskStore.load(cacheKeys);
      hitNum += cachedBitsets.size();
      return cachedBitsets;
    } else {
      throw new IllegalArgumentException("Illegal storageLevel: " + storageLevel);
    }
  }

  public void addToCacheBitsets(CacheKey cacheKey, BitSet bitSet, String storageLevel) {
    CacheEntry cacheEntry = new CacheEntry(cacheKey, bitSet);
    toCacheBitsets.put(cacheEntry, storageLevel);
  }

  public void caching() throws IOException {
    for (Map.Entry<CacheEntry, String> entry : toCacheBitsets.entrySet()) {
      CacheEntry cacheEntry = entry.getKey();
      put(cacheEntry.getCacheKey(), cacheEntry.getBitSet(), entry.getValue());
    }
  }
}
