package com.nus.cool.core.io.cache;

import com.google.common.collect.Maps;
import com.nus.cool.core.io.cache.utils.BubbleLRULinkedHashMap;
import com.nus.cool.core.io.cache.utils.HashMap;
import com.nus.cool.core.util.Range;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class MemoryStore {

  private double memoryCacheSize;  // bits

  private double entryCacheLimit;  // bits

  private double usedMemorySize; // bits

  //  private Map<CacheKey, BitSet> entries = new LinkedHashMap<>(16, 0.75f, true);
  private Map<CacheKey, BitSet> entries = new BubbleLRULinkedHashMap<>(16, 0.75f, true);

  private Map<CacheKeyPrefix, SortedSet<Range>> rangeCacheKeys = new java.util.HashMap<>();

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
      if (cacheKey.getType() == CacheKeyType.VALUE) {
        if (entries.containsKey(cacheKey)) {
          BitSet bitSet = entries.get(cacheKey);
          cachedBitsets.put(cacheKey, bitSet);
        }
      } else {
        CacheKeyPrefix prefix = new CacheKeyPrefix(cacheKey);
        if (rangeCacheKeys.containsKey(prefix)) {
          SortedSet<Range> rangeSet = rangeCacheKeys.get(prefix);

          // Get candidate CacheKeys (exact/partial/subsuming)
          Set<CacheKey> candidateKeys = new HashSet<>();
          int min = cacheKey.getRange().getMin();
          int max = cacheKey.getRange().getMax();
          for (Range range : rangeSet) {
            if (range.getMin() >= max) {
              break;
            }
            if ((range.getMin() <= min && range.getMax() >= max) ||
                (range.getMin() >= min && range.getMax() <= max)) {
              CacheKey candidateKey = new CacheKey(prefix, range);
              candidateKeys.add(candidateKey);
            }
          }

          // Load candidate Bitsets from memory cache
          for (CacheKey key : candidateKeys) {
            if (entries.containsKey(key)) {
              BitSet bitSet = entries.get(key);
              cachedBitsets.put(key, bitSet);
            }
          }
        }
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
    if (cacheKey.getType() == CacheKeyType.TIME || cacheKey.getType() == CacheKeyType.FILTER) {
      CacheKeyPrefix prefix = new CacheKeyPrefix(cacheKey);
      if (rangeCacheKeys.containsKey(prefix)) {
        rangeCacheKeys.get(prefix).add(cacheKey.getRange());
      }
      else {
        SortedSet<Range> rangeSet = new TreeSet<Range>(new Comparator<Range>() {
          @Override
          public int compare(Range o1, Range o2) {
            if (o1.getMin() != o2.getMin()) {
              return o1.getMin() - o2.getMin();
            } else {
              return o1.getMax() - o2.getMax();
            }
          }
        });
        rangeCacheKeys.put(prefix, rangeSet);
      }
    }

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

      // Remove from rangeCacheKeys
      CacheKeyPrefix prefix = new CacheKeyPrefix(entry.getKey());
      if (rangeCacheKeys.containsKey(prefix)) {
        rangeCacheKeys.get(prefix).remove(entry.getKey().getRange());
      }

      if (freeSize >= size) {
        break;
      }
    }
    usedMemorySize -= freeSize;
    return evictedBitsets;
  }
}
