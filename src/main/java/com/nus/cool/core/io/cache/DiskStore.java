package com.nus.cool.core.io.cache;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.nus.cool.core.io.compression.SimpleBitSetCompressor;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DiskStore {

  private static File cacheRoot;

  private static LinkedHashMap<CacheKey, Integer> blockSizes = new LinkedHashMap<>(32, 0.75f, true);

  public DiskStore(String path) {
    cacheRoot = new File(path);
  }

  public void put(CacheKey cacheKey, BitSet bitSet) throws IOException {
    File cacheFile = new File(cacheRoot, cacheKey.getFileName());
    DataOutputStream out = new DataOutputStream(new FileOutputStream(cacheFile));
    int bytesWritten = SimpleBitSetCompressor.compress(bitSet, out);
    blockSizes.put(cacheKey, bytesWritten);
  }

  public Map<Integer, BitSet> load(List<CacheKey> cacheKeys) throws IOException {
    Map<Integer, BitSet> cachedBitsets = Maps.newLinkedHashMap();
    for (CacheKey cacheKey : cacheKeys) {
      File cacheFile = new File(cacheRoot, cacheKey.getFileName());
      if (cacheFile.exists()) {
        ByteBuffer buffer = Files.map(cacheFile).order(ByteOrder.nativeOrder());
        BitSet bitSet = SimpleBitSetCompressor.read(buffer);
        cachedBitsets.put(cacheKey.getLocalID(), bitSet);
      }
    }
    return cachedBitsets;
  }
}
