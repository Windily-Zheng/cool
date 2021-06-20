package com.nus.cool.core.io.cache;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import com.nus.cool.core.io.cache.utils.BubbleLRULinkedHashMap;
import com.nus.cool.core.io.cache.utils.HashMap;
import com.nus.cool.core.util.Range;
import com.nus.cool.core.util.RangeCase;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
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

  // Output cache access statistics
  private boolean printStatistics; // Whether to enable cache access statistics output
  private File outPath; // The output path of cache access statistics
  private FileOutputStream out;
  private OutputStreamWriter osw;
  private BufferedWriter bw;

  public MemoryStore(String path, double memoryCacheSize, double entryCacheLimit)
      throws IOException {
    // TODO: Whether to enable cache access statistics output
    this.printStatistics = false;

    if (memoryCacheSize <= 0) {
      throw new IllegalArgumentException("Illegal memoryCacheSize: " + memoryCacheSize);
    }
    this.memoryCacheSize = memoryCacheSize * 8; // bytes => bits

    if (entryCacheLimit <= 0 || entryCacheLimit > 1) {
      throw new IllegalArgumentException("Illegal entryCacheLimit: " + entryCacheLimit);
    }
    this.entryCacheLimit = entryCacheLimit * memoryCacheSize * 8; // percentage => bits

    this.usedMemorySize = 0;

    // Output cache access statistics
    if (printStatistics) {
      checkNotNull(path);
      // TODO: The output file name
      this.outPath = new File(path, "iceberg-hybrid-237G-5G.csv");
      this.out = new FileOutputStream(outPath);
      this.osw = new OutputStreamWriter(out, "UTF-8");
      // BOM
      osw.write(new String(new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}));
      this.bw = new BufferedWriter(osw);
      String title = "Timestamp(ns),CacheKey,Operation,Result,BitsetSize(Bit),UsedMemorySize(KB)";
      bw.append(title).append("\r");
    } else {
      this.outPath = null;
      this.out = null;
      this.osw = null;
      this.bw = null;
    }
  }

  public Map<CacheKey, BitSet> load(List<CacheKey> cacheKeys) throws IOException {
    Map<CacheKey, BitSet> cachedBitsets = Maps.newLinkedHashMap();

    for (CacheKey cacheKey : cacheKeys) {
      boolean result = false;
      if (cacheKey.getType() == CacheKeyType.VALUE) {
        if (entries.containsKey(cacheKey)) {
          BitSet bitSet = entries.get(cacheKey);
          cachedBitsets.put(cacheKey, bitSet);

          // Output cache access statistics
          result = true;
          if (printStatistics) {
            double memorySize = usedMemorySize / 8 / 1024;
            bw.append(System.nanoTime() + "," + cacheKey + ",query,hit,null," + memorySize + "\r");
            bw.append(System.nanoTime() + "," + cacheKey + ",load,null," + bitSet.size() + ","
                + memorySize + "\r");
          }
        }
        // Output cache access statistics
        if (printStatistics && !result) {
          double memorySize = usedMemorySize / 8 / 1024;
          bw.append(System.nanoTime() + "," + cacheKey + ",query,miss,null," + memorySize + "\r");
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
            if (entries.containsKey(key)) {
              BitSet bitSet = entries.get(key);
              cachedBitsets.put(key, bitSet);
            }
          }
        }

        // Output cache access statistics
        if (printStatistics) {
          double memorySize = usedMemorySize / 8 / 1024;
          // Hit
          if (cachedBitsets.size() > 0) {
            bw.append(System.nanoTime() + "," + cacheKey + ",query,hit,null," + memorySize + "\r");
            for (Map.Entry<CacheKey, BitSet> entry : cachedBitsets.entrySet()) {
              bw.append(
                  System.nanoTime() + "," + entry.getKey() + ",load,null," + entry.getValue().size()
                      + "," + memorySize + "\r");
            }
          }
          // Miss (no reuse)
          else {
            bw.append(System.nanoTime() + "," + cacheKey + ",query,miss,null," + memorySize + "\r");
          }
        }
      }
    }

    // Output cache access statistics
    if (printStatistics) {
      bw.flush(); // flush the data to the output file
    }
    return cachedBitsets;
  }

  public Map<CacheKey, BitSet> put(CacheKey cacheKey, BitSet bitSet) throws IOException {
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

    usedMemorySize += bitSet.size();

    // Output cache access statistics
    if (printStatistics) {
      double memorySize = usedMemorySize / 8 / 1024;
      bw.append(System.nanoTime() + "," + cacheKey + ",put,null," + bitSet.size() + "," + memorySize
          + "\r");
      bw.flush(); // flush the data to the output file
    }

    return evictedBitsets;
  }

  public boolean remove(CacheKey cacheKey) throws IOException {
    if (entries.containsKey(cacheKey)) {
      usedMemorySize -= entries.get(cacheKey).size();

      // Output cache access statistics
      if (printStatistics) {
        double memorySize = usedMemorySize / 8 / 1024;
        bw.append(
            System.nanoTime() + "," + cacheKey + ",remove,null," + entries.get(cacheKey).size() + ","
                + memorySize + "\r");
        bw.flush(); // flush the data to the output file
      }

      entries.remove(cacheKey);

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

  private Map<CacheKey, BitSet> evict(double size) throws IOException {
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
        if (rangeCacheKeys.get(prefix).isEmpty()) {
          rangeCacheKeys.remove(prefix);
        }
      }

      // Output cache access statistics
      if (printStatistics) {
        double memorySize = (usedMemorySize - freeSize) / 8 / 1024;
        bw.append(
            System.nanoTime() + "," + entry.getKey() + ",evict,null," + entry.getValue().size()
                + "," + memorySize + "\r");
      }

      if (freeSize >= size) {
        break;
      }
    }
    usedMemorySize -= freeSize;

    // Output cache access statistics
    if (printStatistics) {
      bw.flush(); // flush the data to the output file
    }

    return evictedBitsets;
  }
}
