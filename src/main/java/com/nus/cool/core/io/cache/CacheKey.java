package com.nus.cool.core.io.cache;

import com.nus.cool.core.util.Range;
import lombok.Getter;

public class CacheKey {

  @Getter
  private CacheKeyType type;

  @Getter
  private String cubletFileName;

  @Getter
  private String fieldName;

  @Getter
  private int chunkID;

  @Getter
  private int localID;

  @Getter
  private Range range;

  public CacheKey(String cubletFileName, String fieldName, int chunkID, int localID) {
    this.type = CacheKeyType.VALUE;
    this.cubletFileName = cubletFileName;
    this.fieldName = fieldName;
    this.chunkID = chunkID;
    this.localID = localID;
  }

  public CacheKey(CacheKeyType type, String cubletFileName, String fieldName, int chunkID,
      Range range) {
    if (type != CacheKeyType.TIME && type != CacheKeyType.FILTER) {
      throw new IllegalArgumentException("Mismatched CacheKey type with TIME/FILTER: " + type);
    }
    this.type = type;
    this.cubletFileName = cubletFileName;
    this.fieldName = fieldName;
    this.chunkID = chunkID;
    this.range = range;
  }

  public CacheKey(CacheKeyPrefix prefix, Range range) {
    if (prefix.getType() != CacheKeyType.TIME && prefix.getType() != CacheKeyType.FILTER) {
      throw new IllegalArgumentException(
          "Mismatched CacheKey type with TIME/FILTER: " + prefix.getType());
    }
    this.type = prefix.getType();
    this.cubletFileName = prefix.getCubletFileName();
    this.fieldName = prefix.getFieldName();
    this.chunkID = prefix.getChunkID();
    this.range = range;
  }

  public CacheKey(String cacheFileName) {
    String fileName = cacheFileName.substring(0, cacheFileName.length() - 3);
    String[] s = fileName.split("_");
    this.type = CacheKeyType.fromInteger(Integer.parseInt(s[0]));
    this.cubletFileName = s[1];
    this.fieldName = s[2];
    this.chunkID = Integer.parseInt(s[3]);
    if (type == CacheKeyType.VALUE) {
      this.localID = Integer.parseInt(s[4]);
    } else {
      this.range = new Range(s[4]);
    }
  }

  public String getFileName() {
    if (type == CacheKeyType.VALUE) {
      return type.ordinal() + "_" + cubletFileName + "_" + fieldName + "_" + chunkID + "_" + localID
          + ".dz";
    } else {
      return type.ordinal() + "_" + cubletFileName + "_" + fieldName + "_" + chunkID + "_" + range
          .toString() + ".dz";
    }
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + type.hashCode();
    result = 31 * result + cubletFileName.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + chunkID;
    if (type == CacheKeyType.VALUE) {
      result = 31 * result + localID;
    } else {
      result = 31 * result + range.hashCode();
    }
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
    if (!this.type.equals(cacheKey.getType())) {
      return false;
    }
    if (this.cubletFileName.equals(cacheKey.getCubletFileName()) && this.fieldName
        .equals(cacheKey.getFieldName()) && cacheKey.getChunkID() == this.chunkID) {
      if (this.type == CacheKeyType.VALUE) {
        return cacheKey.getLocalID() == this.localID;
      } else {
        return this.range.equals(cacheKey.getRange());
      }
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    if (type == CacheKeyType.VALUE) {
      return String
          .format("type = %s, cublet = %s, field = %s, chunkID = %d, localID = %d", type.toString(),
              cubletFileName, fieldName, chunkID, localID);
    } else {
      return String
          .format("type = %s, cublet = %s, field = %s, chunkID = %d, range = %s", type.toString(),
              cubletFileName, fieldName, chunkID, range.toString());
    }
  }
}
