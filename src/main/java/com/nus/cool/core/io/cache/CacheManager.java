package com.nus.cool.core.io.cache;


import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
      double entryCacheLimit) throws IOException {
    memoryStore = new MemoryStore(memoryCacheSize, entryCacheLimit);
    diskStore = new DiskStore(cacheRoot, diskCacheSize, entryCacheLimit);
    toCacheBitsets = Maps.newHashMap();
    hitNum = 0;
    totalNum = 0;
    initMemoryFromDisk();
  }

  public void put(CacheKey cacheKey, BitSet bitSet, String storageLevel) throws IOException {
    checkNotNull(storageLevel);
    if ("MEMORY_ONLY".equals(storageLevel)) {
      memoryStore.put(cacheKey, bitSet);
    } else if ("DISK_ONLY".equals(storageLevel)) {
      diskStore.put(cacheKey, bitSet);
    } else if ("MEMORY_AND_DISK".equals(storageLevel)) {
      Map<CacheKey, BitSet> evictedBitsets = memoryStore.put(cacheKey, bitSet);
      if (evictedBitsets.size() > 0) {
        System.out.println("Putting " + evictedBitsets.size() + " bitsets into disk cache");
        for (Map.Entry<CacheKey, BitSet> entry : evictedBitsets.entrySet()) {
          diskStore.put(entry.getKey(), entry.getValue());
        }
      }
    } else {
      throw new IllegalArgumentException("Illegal storageLevel: " + storageLevel);
    }
  }

  public Map<CacheKey, BitSet> load(List<CacheKey> cacheKeys, String storageLevel)
      throws IOException {
    checkNotNull(storageLevel);
    totalNum += cacheKeys.size();
    if ("MEMORY_ONLY".equals(storageLevel)) {
      Map<CacheKey, BitSet> cachedBitsets = memoryStore.load(cacheKeys);
      hitNum += cachedBitsets.size();
      return cachedBitsets;
    } else if ("DISK_ONLY".equals(storageLevel)) {
      Map<CacheKey, BitSet> cachedBitsets = diskStore.load(cacheKeys);
      hitNum += cachedBitsets.size();
      return cachedBitsets;
    } else if ("MEMORY_AND_DISK".equals(storageLevel)) {
      Map<CacheKey, BitSet> cachedBitsets = memoryStore.load(cacheKeys);
      // Only count the hit number of memory cache
      hitNum += cachedBitsets.size();
      if (cachedBitsets.size() < cacheKeys.size()) {
        List<CacheKey> missingCacheKeys = Lists.newArrayList();
        for (CacheKey cacheKey : cacheKeys) {
          if (!cachedBitsets.containsKey(cacheKey)) {
            missingCacheKeys.add(cacheKey);
          }
        }
        Map<CacheKey, BitSet> diskCachedBitsets = diskStore.load(missingCacheKeys);
        for (Map.Entry<CacheKey, BitSet> entry : diskCachedBitsets.entrySet()) {
          cachedBitsets.put(entry.getKey(), entry.getValue());
          // Put missing entries into memory cache
          Map<CacheKey, BitSet> evictedBitsets = memoryStore.put(entry.getKey(), entry.getValue());
          if (evictedBitsets.size() > 0) {
            for (Map.Entry<CacheKey, BitSet> en : evictedBitsets.entrySet()) {
              diskStore.put(en.getKey(), en.getValue());
            }
          }
        }
      }
//      hitNum += cachedBitsets.size();
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

  private void initMemoryFromDisk() throws IOException {
    Set<CacheKey> diskCachedKeys = diskStore.getCachedKeys();
    List<CacheKey> cacheKeys = Lists.newArrayList(diskCachedKeys);
    Map<CacheKey, BitSet> diskCachedBitsets = diskStore.load(cacheKeys);
    if (cacheKeys.size() != diskCachedBitsets.size()) {
      throw new RuntimeException(
          "Size of cacheKeys(" + cacheKeys.size() + ") is not equal to size of diskCachedBitsets("
              + diskCachedBitsets.size() + ")");
    }
    Map<CacheKey, BitSet> evictedBitsets;
    for (Map.Entry<CacheKey, BitSet> entry : diskCachedBitsets.entrySet()) {
      evictedBitsets = memoryStore.put(entry.getKey(), entry.getValue());
      if (evictedBitsets.size() > 0) {
        break;
      }
    }
  }
}
