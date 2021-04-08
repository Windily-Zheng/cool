package com.nus.cool.core.io.cache;

import lombok.Getter;

public class CacheKeyPrefix {

  @Getter
  private CacheKeyType type;

  @Getter
  private String cubletFileName;

  @Getter
  private String fieldName;

  @Getter
  private int chunkID;

  public CacheKeyPrefix(CacheKeyType type, String cubletFileName, String fieldName, int chunkID) {
    this.type = type;
    this.cubletFileName = cubletFileName;
    this.fieldName = fieldName;
    this.chunkID = chunkID;
  }

  public CacheKeyPrefix(CacheKey cacheKey) {
    this.type = cacheKey.getType();
    this.cubletFileName = cacheKey.getCubletFileName();
    this.fieldName = cacheKey.getFieldName();
    this.chunkID = cacheKey.getChunkID();
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + type.hashCode();
    result = 31 * result + cubletFileName.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + chunkID;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CacheKey)) {
      return false;
    }
    CacheKey cacheKey = (CacheKey) o;
    return this.type.equals(cacheKey.getType()) && this.cubletFileName
        .equals(cacheKey.getCubletFileName()) && this.fieldName.equals(cacheKey.getFieldName())
        && cacheKey.getChunkID() == this.chunkID;
  }

  @Override
  public String toString() {
    return String
        .format("type = %s, cublet = %s, field = %s, chunkID = %d", type.toString(), cubletFileName,
            fieldName, chunkID);
  }
}
