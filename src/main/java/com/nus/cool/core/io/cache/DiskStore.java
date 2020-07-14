package com.nus.cool.core.io.cache;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.nus.cool.core.io.compression.SimpleBitSetCompressor;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class DiskStore {

  private File cacheRoot;

  private double diskCacheSize;

  private double entryCacheLimit;

  private double usedDiskSize;

  private Map<CacheKey, Integer> blockSizes = new LinkedHashMap<>(16, 0.75f, true);

  public DiskStore(String path, double diskCacheSize, double entryCacheLimit) {
    checkNotNull(path);
    this.cacheRoot = new File(path);

    if (diskCacheSize <= 0) {
      throw new IllegalArgumentException("Illegal diskCacheSize: " + diskCacheSize);
    }
    this.diskCacheSize = diskCacheSize;

    if (entryCacheLimit <= 0 || entryCacheLimit > 1) {
      throw new IllegalArgumentException("Illegal entryCacheLimit: " + entryCacheLimit);
    }
    this.entryCacheLimit = entryCacheLimit * diskCacheSize;

    this.usedDiskSize = 0;

    init();
  }

  private void init() {
    File[] cacheFiles = cacheRoot.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File file, String s) {
        return s.endsWith(".dz");
      }
    });
    checkNotNull(cacheFiles);
    for (File cacheFile : cacheFiles) {
      CacheKey cacheKey = new CacheKey(cacheFile.getName());
      int blockSize = (int) cacheFile.length();
      blockSizes.put(cacheKey, blockSize);
      usedDiskSize += blockSize;
    }
    if (usedDiskSize > diskCacheSize) {
      evict(usedDiskSize - diskCacheSize);
    }
  }

  public Map<CacheKey, BitSet> load(List<CacheKey> cacheKeys) throws IOException {
    Map<CacheKey, BitSet> cachedBitsets = Maps.newLinkedHashMap();
    for (CacheKey cacheKey : cacheKeys) {
      if (blockSizes.containsKey(cacheKey)) {
        blockSizes.get(cacheKey);
        File cacheFile = new File(cacheRoot, cacheKey.getFileName());
        if (cacheFile.exists()) {
          ByteBuffer buffer = Files.map(cacheFile).order(ByteOrder.nativeOrder());
          BitSet bitSet = SimpleBitSetCompressor.read(buffer);
          cachedBitsets.put(cacheKey, bitSet);
        }
      }
    }
    return cachedBitsets;
  }

  public void put(CacheKey cacheKey, BitSet bitSet) throws IOException {
    if (blockSizes.containsKey(cacheKey)) {
      return;
    }
    File cacheFile = new File(cacheRoot, cacheKey.getFileName());
    DataOutputStream out = new DataOutputStream(new FileOutputStream(cacheFile));
    int bytesWritten = SimpleBitSetCompressor.compress(bitSet, out);

    if (bytesWritten >= entryCacheLimit) {
      cacheFile.delete();
      System.out.println(
          "Exceed entryCacheLimit! CacheKey(" + cacheKey.toString() + ") not cached on disk!");
      return;
    }

    blockSizes.put(cacheKey, bytesWritten);
    usedDiskSize += bytesWritten;

    if (usedDiskSize > diskCacheSize) {
      evict(usedDiskSize - diskCacheSize);
    }
  }

  private void evict(double size) {
    int freeSize = 0;
    // LRU
    for (Iterator<Entry<CacheKey, Integer>> it = blockSizes.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<CacheKey, Integer> entry = it.next();
      CacheKey cacheKey = entry.getKey();
      File cacheFile = new File(cacheRoot, cacheKey.getFileName());
      if (cacheFile.exists()) {
        cacheFile.delete();
      }
      freeSize += entry.getValue();
      it.remove();
      if (freeSize >= size) {
        break;
      }
    }
    usedDiskSize -= freeSize;
  }

  public Set<CacheKey> getCachedKeys() {
    return blockSizes.keySet();
  }
}
