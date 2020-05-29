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

public class DiskStore {

  private File cacheRoot;

  private int diskCacheSize;

  private int entryCacheLimit;

  private int usedDiskSize;

  private Map<CacheKey, Integer> blockSizes = new LinkedHashMap<>(16, 0.75f, true);

  public DiskStore(String path, int diskCacheSize, double entryCacheLimit) {
    checkNotNull(path);
    this.cacheRoot = new File(path);

    if (diskCacheSize <= 0) {
      throw new IllegalArgumentException("Illegal diskCacheSize: " + diskCacheSize);
    }
    this.diskCacheSize = diskCacheSize;

    if (entryCacheLimit <= 0 || entryCacheLimit > 1) {
      throw new IllegalArgumentException("Illegal entryCacheLimit: " + entryCacheLimit);
    }
    this.entryCacheLimit = (int) (entryCacheLimit * diskCacheSize);

    this.usedDiskSize = 0;

    System.out.println("diskCacheSize: " + this.diskCacheSize);
    System.out.println("entryCacheLimit: " + this.entryCacheLimit);
    System.out.println("*** Initialize disk cache ***");

    init();
    System.out.println("usedDiskSize: " + usedDiskSize);
    System.out.println();
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
      System.out.println("CacheKey(" + cacheKey.toString() + ") Size: " + blockSize);
    }
    if (usedDiskSize > diskCacheSize) {
      evict(usedDiskSize - diskCacheSize);
    }
  }

  public Map<Integer, BitSet> load(List<CacheKey> cacheKeys) throws IOException {
    Map<Integer, BitSet> cachedBitsets = Maps.newLinkedHashMap();
    for (CacheKey cacheKey : cacheKeys) {
      if (blockSizes.containsKey(cacheKey)) {
        blockSizes.get(cacheKey);
        File cacheFile = new File(cacheRoot, cacheKey.getFileName());
        if (cacheFile.exists()) {
          ByteBuffer buffer = Files.map(cacheFile).order(ByteOrder.nativeOrder());
          BitSet bitSet = SimpleBitSetCompressor.read(buffer);
          cachedBitsets.put(cacheKey.getLocalID(), bitSet);
        }
      }
    }
    return cachedBitsets;
  }

  public void put(CacheKey cacheKey, BitSet bitSet) throws IOException {
    File cacheFile = new File(cacheRoot, cacheKey.getFileName());
    DataOutputStream out = new DataOutputStream(new FileOutputStream(cacheFile));
    int bytesWritten = SimpleBitSetCompressor.compress(bitSet, out);
    System.out.println("bytesWritten: " + bytesWritten);

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

  private void evict(int size) {
    System.out.println("*** Evicting ***");
    System.out.println("usedDiskSize: " + usedDiskSize);
    System.out.println("Space needed to free: " + size);

    int freeSize = 0;
    // LRU
    for (Iterator<Entry<CacheKey, Integer>> it = blockSizes.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<CacheKey, Integer> entry = it.next();
      CacheKey cacheKey = entry.getKey();
      File cacheFile = new File(cacheRoot, cacheKey.getFileName());
      if (cacheFile.exists()) {
        System.out
            .println("Evict file: " + cacheKey.getFileName() + " (size: " + entry.getValue() + ")");
        cacheFile.delete();
      }
      freeSize += entry.getValue();
      it.remove();
      if (freeSize >= size) {
        break;
      }
    }
    usedDiskSize -= freeSize;

    System.out.println("Total free size: " + freeSize);
    System.out.println();
  }
}
