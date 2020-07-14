package com.nus.cool.core.io.cache;

import com.google.common.collect.Maps;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MemoryStore {

  private double memoryCacheSize;  // bits

  private double entryCacheLimit;  // bits

  private double usedMemorySize; // bits

  public Map<CacheKey, BitSet> entries = new LinkedHashMap<>(16, 0.75f, true);

  public MemoryStore(double memoryCacheSize, double entryCacheLimit) {
    if (memoryCacheSize <= 0) {
      throw new IllegalArgumentException("Illegal memoryCacheSize: " + memoryCacheSize);
    }
    this.memoryCacheSize = memoryCacheSize * 8; // bytes => bits

    if (entryCacheLimit <= 0 || entryCacheLimit > 1) {
      throw new IllegalArgumentException("Illegal entryCacheLimit: " + entryCacheLimit);
    }
    this.entryCacheLimit = entryCacheLimit * memoryCacheSize * 8; // percentage => bits

    this.usedMemorySize = 0;
  }

  public Map<CacheKey, BitSet> load(List<CacheKey> cacheKeys) {
    Map<CacheKey, BitSet> cachedBitsets = Maps.newLinkedHashMap();
    for (CacheKey cacheKey : cacheKeys) {
      if (entries.containsKey(cacheKey)) {
        BitSet bitSet = entries.get(cacheKey);
        cachedBitsets.put(cacheKey, bitSet);
      }
    }
    return cachedBitsets;
  }

  public Map<CacheKey, BitSet> put(CacheKey cacheKey, BitSet bitSet) {
    Map<CacheKey, BitSet> evictedBitsets = Maps.newLinkedHashMap();

    if (entries.containsKey(cacheKey)) {
      return evictedBitsets;
    }

    if (bitSet.size() >= entryCacheLimit) {
      System.out.println(
          "Exceed entryCacheLimit! CacheKey(" + cacheKey.toString() + ") not cached in memory!");
      evictedBitsets.put(cacheKey, bitSet);
      return evictedBitsets;
    }

    if (usedMemorySize + bitSet.size() > memoryCacheSize) {
      evictedBitsets = evict(usedMemorySize + bitSet.size() - memoryCacheSize);
    }

    entries.put(cacheKey, bitSet);
    usedMemorySize += bitSet.size();
    return evictedBitsets;
  }

  private Map<CacheKey, BitSet> evict(double size) {
    int freeSize = 0;
    Map<CacheKey, BitSet> evictedBitsets = Maps.newLinkedHashMap();

    // LRU
    for (Iterator<Entry<CacheKey, BitSet>> it = entries.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<CacheKey, BitSet> entry = it.next();
      evictedBitsets.put(entry.getKey(), entry.getValue());
      freeSize += entry.getValue().size();
      it.remove();
      if (freeSize >= size) {
        break;
      }
    }
    usedMemorySize -= freeSize;
    return evictedBitsets;
  }
}
