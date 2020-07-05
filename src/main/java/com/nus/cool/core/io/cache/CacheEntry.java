package com.nus.cool.core.io.cache;

import java.util.BitSet;
import lombok.Getter;

public class CacheEntry {

  @Getter
  private CacheKey cacheKey;

  @Getter
  private BitSet bitSet;

  public CacheEntry(CacheKey cacheKey, BitSet bitSet) {
    this.cacheKey = cacheKey;
    this.bitSet = bitSet;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + cacheKey.hashCode();
    result = 31 * result + bitSet.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CacheEntry)) {
      return false;
    }
    CacheEntry cacheEntry = (CacheEntry) o;
    return this.cacheKey.equals(cacheEntry.getCacheKey()) && this.bitSet
        .equals(cacheEntry.getBitSet());
  }

  @Override
  public String toString() {
    return String.format("cacheKey = (%s), bitset = (%s)", cacheKey.toString(), bitSet.toString());
  }
}
