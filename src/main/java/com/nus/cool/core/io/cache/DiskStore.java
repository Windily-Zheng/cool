package com.nus.cool.core.io.cache;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.nus.cool.core.io.cache.utils.BubbleLRULinkedHashMap;
import com.nus.cool.core.io.compression.SimpleBitSetCompressor;

import com.nus.cool.core.util.Range;
import com.nus.cool.core.util.RangeCase;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

public class DiskStore {

  private File cacheRoot;

  private double diskCacheSize;

  private double entryCacheLimit;

  private double usedDiskSize;

    private Map<CacheKey, Integer> blockSizes = new LinkedHashMap<>(16, 0.75f, true);
//  private Map<CacheKey, Integer> blockSizes = new BubbleLRULinkedHashMap<>(16, 0.75f, true);

  private Map<CacheKeyPrefix, SortedSet<Range>> rangeCacheKeys = new java.util.HashMap<>();

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

      if (cacheKey.getType() == CacheKeyType.TIME || cacheKey.getType() == CacheKeyType.FILTER) {
        CacheKeyPrefix prefix = new CacheKeyPrefix(cacheKey);
        if (rangeCacheKeys.containsKey(prefix)) {
          rangeCacheKeys.get(prefix).add(cacheKey.getRange());
        } else {
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
          rangeSet.add(cacheKey.getRange());
          rangeCacheKeys.put(prefix, rangeSet);
        }
      }
    }
    if (usedDiskSize > diskCacheSize) {
      evict(usedDiskSize - diskCacheSize);
    }
  }

  public Map<CacheKey, BitSet> load(List<CacheKey> cacheKeys) throws IOException {
    Map<CacheKey, BitSet> cachedBitsets = Maps.newLinkedHashMap();
    for (CacheKey cacheKey : cacheKeys) {
      if (cacheKey.getType() == CacheKeyType.VALUE) {
        if (blockSizes.containsKey(cacheKey)) {
          blockSizes.get(cacheKey);
          File cacheFile = new File(cacheRoot, cacheKey.getFileName());
          if (cacheFile.exists()) {
            ByteBuffer buffer = Files.map(cacheFile).order(ByteOrder.nativeOrder());
            BitSet bitSet = SimpleBitSetCompressor.read(buffer);
            cachedBitsets.put(cacheKey, bitSet);
          }
        }
      } else {
        CacheKeyPrefix prefix = new CacheKeyPrefix(cacheKey);
        if (rangeCacheKeys.containsKey(prefix)) {
          SortedSet<Range> rangeSet = rangeCacheKeys.get(prefix);

          // Get candidate CacheKeys (exact/partial/subsuming)
          Set<CacheKey> candidateKeys = new HashSet<>();
          Range searchedRange = cacheKey.getRange();
          Range minSubsuming = null;
          for (Range range : rangeSet) {
            if (range.getMin() >= searchedRange.getMax()) {
              break;
            }
            // Exact Reuse Case
            if (searchedRange.compareTo(range) == RangeCase.EXACT) {
              // Clear potential other reuse cases stored previously
              candidateKeys.clear();
              CacheKey candidateKey = new CacheKey(prefix, range);
              candidateKeys.add(candidateKey);
              break;
            }
            // Subsuming Reuse Case
            if (searchedRange.compareTo(range) == RangeCase.SUBSUMING) {
              CacheKey candidateKey = new CacheKey(prefix, range);
              /* Clear potential partial reuse cases stored previously ||
                 Replace subsuming range stored previously with a smaller range */
              if (minSubsuming == null || range.getLength() < minSubsuming.getLength()) {
                candidateKeys.clear();
                minSubsuming = range;
                candidateKeys.add(candidateKey);
              }
            }
            // Partial Reuse Case
            if (searchedRange.compareTo(range) == RangeCase.PARTIAL) {
              CacheKey candidateKey = new CacheKey(prefix, range);
              candidateKeys.add(candidateKey);
            }
          }

          // Load candidate Bitsets from memory cache
          for (CacheKey key : candidateKeys) {
            if (blockSizes.containsKey(key)) {
              blockSizes.get(key);
              File cacheFile = new File(cacheRoot, key.getFileName());
              if (cacheFile.exists()) {
                ByteBuffer buffer = Files.map(cacheFile).order(ByteOrder.nativeOrder());
                BitSet bitSet = SimpleBitSetCompressor.read(buffer);
                cachedBitsets.put(key, bitSet);
              }
            }
          }
        }
      }
    }
    return cachedBitsets;
  }

  public Map<CacheKey, BitSet> loadExact(List<CacheKey> cacheKeys) throws IOException {
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

    if (cacheKey.getType() == CacheKeyType.TIME || cacheKey.getType() == CacheKeyType.FILTER) {
      CacheKeyPrefix prefix = new CacheKeyPrefix(cacheKey);
      if (rangeCacheKeys.containsKey(prefix)) {
        rangeCacheKeys.get(prefix).add(cacheKey.getRange());
      } else {
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
        rangeSet.add(cacheKey.getRange());
        rangeCacheKeys.put(prefix, rangeSet);
      }
    }

    if (usedDiskSize > diskCacheSize) {
      evict(usedDiskSize - diskCacheSize);
    }
  }

  public boolean remove(CacheKey cacheKey) {
    if (blockSizes.containsKey(cacheKey)) {
      usedDiskSize -= blockSizes.get(cacheKey);
      blockSizes.remove(cacheKey);
      File cacheFile = new File(cacheRoot, cacheKey.getFileName());
      if (cacheFile.exists()) {
        cacheFile.delete();
      }

      // Remove from rangeCacheKeys
      CacheKeyPrefix prefix = new CacheKeyPrefix(cacheKey);
      if (rangeCacheKeys.containsKey(prefix)) {
        rangeCacheKeys.get(prefix).remove(cacheKey.getRange());
        if (rangeCacheKeys.get(prefix).isEmpty()) {
          rangeCacheKeys.remove(prefix);
        }
      }
      return true;
    }
    return false;
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

      // Remove from rangeCacheKeys
      CacheKeyPrefix prefix = new CacheKeyPrefix(entry.getKey());
      if (rangeCacheKeys.containsKey(prefix)) {
        rangeCacheKeys.get(prefix).remove(entry.getKey().getRange());
        if (rangeCacheKeys.get(prefix).isEmpty()) {
          rangeCacheKeys.remove(prefix);
        }
      }

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
