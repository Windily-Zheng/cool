package com.nus.cool.core.io.cache;

import lombok.Getter;

public class CacheKey {

  @Getter
  private String cubletFileName;

  @Getter
  private int chunkID;

  @Getter
  private int localID;

  public CacheKey(String cubletFileName, int chunkID, int localID) {
    this.cubletFileName = cubletFileName;
    this.chunkID = chunkID;
    this.localID = localID;
  }

  public String getFileName() {
    return cubletFileName + "_" + chunkID + "_" + localID + ".dz";
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + cubletFileName.hashCode();
    result = 31 * result + chunkID;
    result = 31 * result + localID;
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
    return this.cubletFileName.equals(cacheKey.getCubletFileName()) &&
        cacheKey.getChunkID() == this.chunkID && cacheKey.getLocalID() == this.localID;
  }

  @Override
  public String toString() {
    return String
        .format("cublet = %s, chunkID = %d, localID = %d", cubletFileName, chunkID, localID);
  }
}
