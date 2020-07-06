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

  private Map<CacheKey, BitSet> entries = new LinkedHashMap<>(16, 0.75f, true);

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

//    System.out.println("memoryCacheSize: " + this.memoryCacheSize + " bits");
//    System.out.println("entryCacheLimit: " + this.entryCacheLimit + " bits");
//    System.out.println();
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

  public void put(CacheKey cacheKey, BitSet bitSet) {
    if (bitSet.size() >= entryCacheLimit) {
      System.out.println(
          "Exceed entryCacheLimit! CacheKey(" + cacheKey.toString() + ") not cached in memory!");
      return;
    }

    if (usedMemorySize + bitSet.size() > memoryCacheSize) {
      evict(usedMemorySize + bitSet.size() - memoryCacheSize);
    }

    entries.put(cacheKey, bitSet);
    usedMemorySize += bitSet.size();

//    System.out.println(
//        "Caching Bitset: CacheKey(" + cacheKey.toString() + ") size: " + bitSet.size() + " bits");
//    System.out.println("usedMemorySize: " + usedMemorySize);
  }

  private void evict(double size) {
    // TODO: Need to evict to disk cache (MEMORY_AND_DISK)
    System.out.println("*** Evicting ***");
    System.out.println("Bits needed to free: " + size);

    int freeSize = 0;
    // LRU
    for (Iterator<Entry<CacheKey, BitSet>> it = entries.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<CacheKey, BitSet> entry = it.next();
//      CacheKey cacheKey = entry.getKey();
//      System.out.println(
//          "Evict Bitset: CacheKey(" + cacheKey.toString() + ") size: " + entry.getValue().size()
//              + " bits");
      freeSize += entry.getValue().size();
      it.remove();
      if (freeSize >= size) {
        break;
      }
    }
    usedMemorySize -= freeSize;

//    System.out.println("Total free size: " + freeSize + " bits");
//    System.out.println();
  }
}
