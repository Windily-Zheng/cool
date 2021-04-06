package com.nus.cool.core.io.cache;

public enum CacheKeyType {

  /**
   * This CacheKey type used for time range cache.
   */
  TIME,

  /**
   * This CacheKey type used for filter cache.
   */
  FILTER,

  /**
   * This CacheKey type used for value cache.
   */
  VALUE;

  public static CacheKeyType fromInteger(int i) {
    switch (i) {
      case 0:
        return TIME;
      case 1:
        return FILTER;
      case 2:
        return VALUE;
      default:
        throw new IllegalArgumentException("Invalid CacheKey type int: " + i);
    }
  }
}
