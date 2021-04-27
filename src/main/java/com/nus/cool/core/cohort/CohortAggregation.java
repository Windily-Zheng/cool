/*
 * Copyright 2020 Cool Squad Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nus.cool.core.cohort;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nus.cool.core.cohort.aggregator.Aggregator;
import com.nus.cool.core.cohort.aggregator.SumAggregator;
import com.nus.cool.core.cohort.aggregator.UserCountAggregator;
import com.nus.cool.core.cohort.filter.FieldFilter;
import com.nus.cool.core.cohort.filter.RangeFieldFilter;
import com.nus.cool.core.io.cache.CacheKey;
import com.nus.cool.core.io.cache.CacheKeyType;
import com.nus.cool.core.io.cache.CacheManager;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.RLEInputVector;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.Range;
import com.nus.cool.core.util.RangeCase;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * @author zhongle, hongbin
 * @version 0.1
 * @since 0.1
 */
public class CohortAggregation implements Operator {

  private CohortSelection sigma;

  private TableSchema schema;

  private CohortQuery query;

  private String[] birthActions;

  private int[] birthActionGlobalIDs;

  private int[] bBirthActionChunkIDs;

  @Getter
  private BitSet bs;

  @Getter
  private Map<CohortKey, Long> cubletResults = Maps.newLinkedHashMap();

  // for testing
  public double totalSeekTime;

  // for testing
  public double totalLoadTime;

  // for testing
  public double totalGenerateTime;

  // for testing
  public double totalFilterTime;

  // for testing
  public double totalSelectionTime;

  // for testing
  public double totalAggregationTime;

  // for testing
  public int chunkNum;

  public CohortAggregation(CohortSelection sigma) {
    this.sigma = checkNotNull(sigma);
  }

  @Override
  public void init(TableSchema schema, CohortQuery query) {
    this.schema = checkNotNull(schema);
    this.query = checkNotNull(query);
    this.birthActions = query.getBirthActions();
    this.sigma.init(schema, query);

    // for testing
    totalSeekTime = 0;
    totalLoadTime = 0;
    totalGenerateTime = 0;
    totalFilterTime = 0;
    totalSelectionTime = 0;
    totalAggregationTime = 0;
    chunkNum = 0;
  }

  @Override
  public void process(MetaChunkRS metaChunk) {
    this.sigma.process(metaChunk);

    int actionField = this.schema.getActionField();
    MetaFieldRS actionMetaField = metaChunk.getMetaField(actionField, FieldType.Action);
    this.birthActionGlobalIDs = new int[this.birthActions.length];
    for (int i = 0; i < this.birthActions.length; i++) {
      int id = actionMetaField.find(this.birthActions[i]);
      if (id < 0) {
        throw new RuntimeException("Unknown birth action: " + this.birthActions[i]);
      }
      this.birthActionGlobalIDs[i] = id;
    }
  }

  @Override
  public void process(ChunkRS chunk, boolean reuse, CacheManager cacheManager, String storageLevel,
      String cubletFileName) throws IOException {
    this.sigma.process(chunk, reuse, cacheManager, storageLevel, cubletFileName);
    if (!this.sigma.isBUserActiveChunk() || !this.sigma.isBAgeActiveChunk()) {
      return;
    }

    FieldRS appField = loadField(chunk, this.schema.getAppKeyField());
    FieldRS userField = loadField(chunk, this.schema.getUserKeyField());
    FieldRS actionField = loadField(chunk, this.schema.getActionField());
    FieldRS actionTimeField = loadField(chunk, this.schema.getActionTimeField());
    FieldRS cohortField = loadField(chunk, this.query.getCohortFields()[0]);
    FieldRS metricField = loadField(chunk, this.query.getMetric());
    this.bBirthActionChunkIDs = new int[this.birthActionGlobalIDs.length];
    for (int i = 0; i < this.birthActionGlobalIDs.length; i++) {
      int id = actionField.getKeyVector().find(this.birthActionGlobalIDs[i]);
      if (id < 0) {
        return;
      }
      this.bBirthActionChunkIDs[i] = id;
    }

    int min = cohortField.minKey();
    int cardinality = cohortField.maxKey() - min + 1;
    int cohortSize = actionTimeField.maxKey() - actionTimeField.minKey() + 1 + 1;
    long[][] chunkResults = new long[cardinality][cohortSize];

    InputVector cohortInput = cohortField.getValueVector();
    InputVector actionTimeInput = actionTimeField.getValueVector();
    InputVector metricInput = metricField == null ? null : metricField.getValueVector();
    InputVector actionInput = actionField.getValueVector();
    Aggregator aggregator = newAggregator();
    int minAllowedAge = 0;
    int maxAllowedAge = cohortSize - 1;
    aggregator.init(metricInput, actionTimeInput, cohortSize, minAllowedAge, maxAllowedAge,
        this.query.getAgeInterval());

    RLEInputVector appInput = (RLEInputVector) appField.getValueVector();
    appInput.skipTo(0);
    RLEInputVector.Block appBlock = new RLEInputVector.Block();
    FieldFilter appFilter = this.sigma.getAppFilter();
    BitSet bv = new BitSet(chunk.getRecords());
    this.bs = new BitSet(chunk.getRecords());

    chunkNum++;

    // 1. Load: Load chunk cache
    Set<Integer> birthIDSet = new HashSet<>();
    Map<Integer, BitSet> cachedBirthBitsets = Maps.newLinkedHashMap();
    Map<String, Set<Integer>> ageIDSets = Maps.newLinkedHashMap();
    Map<String, Map<Integer, BitSet>> cachedAgeBitsets = Maps.newLinkedHashMap();
//    BitSet ageMatchedRows = new BitSet(chunk.getRecords());

    if (reuse) {
      long loadStart = System.nanoTime();
      // Construct birth action cacheKeys
//      long birthConstructStart = System.nanoTime();
      List<CacheKey> birthCacheKeys = Lists.newArrayList();
      for (int id : this.bBirthActionChunkIDs) {
        // avoid duplicate cacheKey
        if (!birthIDSet.contains(id)) {
          birthIDSet.add(id);
          CacheKey cacheKey = new CacheKey(cubletFileName, this.schema.getActionFieldName(),
              chunk.getChunkID(), id);
          birthCacheKeys.add(cacheKey);
        }
      }
//      long birthConstructEnd = System.nanoTime();
//      long birthConstructTime = birthConstructEnd - birthConstructStart;
//      System.out.println("Construct CacheKeys: " + birthConstructTime + "ns");

//      System.out.println("*** Loading cached Bitsets ***");
      // Load birth action cache
//      long birthLoadStart = System.nanoTime();
      Map<CacheKey, BitSet> loadBirthBitsets = cacheManager.load(birthCacheKeys, storageLevel);
      for (Map.Entry<CacheKey, BitSet> entry : loadBirthBitsets.entrySet()) {
        cachedBirthBitsets.put(entry.getKey().getLocalID(), entry.getValue());
      }
//      long birthLoadEnd = System.nanoTime();
//      long birthLoadTime = (birthLoadEnd - birthLoadStart);
//      System.out.println("Load chunk cache: " + birthLoadTime + "ns");

      // Construct age selection cacheKeys
//      long ageLoadStart = System.nanoTime();
      Map<String, BitSet> ageFilterBitsets = this.sigma.getAgeFilterBitsets();
      for (Map.Entry<String, BitSet> entry : ageFilterBitsets.entrySet()) {
        List<CacheKey> ageCacheKeys = Lists.newArrayList();
        BitSet filter = entry.getValue();
        Set<Integer> ageIDSet = new HashSet<>();
        int pos = 0;
        pos = filter.nextSetBit(pos);
        while (pos >= 0) {
          ageIDSet.add(pos);
          CacheKey cacheKey = new CacheKey(cubletFileName, entry.getKey(), chunk.getChunkID(), pos);
          ageCacheKeys.add(cacheKey);
          pos = filter.nextSetBit(pos + 1);
        }
        // Load age selection cache
        ageIDSets.put(entry.getKey(), ageIDSet);
        Map<Integer, BitSet> cachedAgeFieldBitsets = Maps.newLinkedHashMap();
        Map<CacheKey, BitSet> loadAgeBitsets = cacheManager.load(ageCacheKeys, storageLevel);
        for (Map.Entry<CacheKey, BitSet> en : loadAgeBitsets.entrySet()) {
          cachedAgeFieldBitsets.put(en.getKey().getLocalID(), en.getValue());
        }
        cachedAgeBitsets.put(entry.getKey(), cachedAgeFieldBitsets);
      }
      long loadEnd = System.nanoTime();
      totalLoadTime += (loadEnd - loadStart);

//      long ageLoadEnd = System.nanoTime();
//      long ageLoadTime = (ageLoadEnd - ageLoadStart);
//      totalLoadTime += (birthConstructTime + birthLoadTime + ageLoadTime);

      // 2. Generate: Generate missing bitsets
      // Generate missing birth bitsets

      long generateStart = System.nanoTime();
      if (cachedBirthBitsets.size() < birthIDSet.size()) {
//        System.out.println("*** Caching missing Bitsets ***");
//        long birthCheckStart = System.nanoTime();
        Map<Integer, BitSet> toCacheBirthBitsets = Maps.newLinkedHashMap();
        // Check missing birth localIDs
        for (int id : birthIDSet) {
          if (!cachedBirthBitsets.containsKey(id)) {
            BitSet bitSet = new BitSet(chunk.getRecords());
            toCacheBirthBitsets.put(id, bitSet);
//            System.out.println("Missing local ID: " + id);
          }
        }
//        long birthCheckEnd = System.nanoTime();
//        long birthCheckTime = birthCheckEnd - birthCheckStart;
//        System.out.println("Check missed localIDs: " + birthCheckTime + "ns");

        // Traverse actionInput to generate missing birth bitsets
//        long birthTraverseStart = System.nanoTime();
        int pos = 0;
        actionInput.skipTo(pos);
        while (actionInput.hasNext()) {
          int key = actionInput.next();
          if (toCacheBirthBitsets.containsKey(key)) {
            toCacheBirthBitsets.get(key).set(pos);
          }
          pos++;
        }
//        long birthTraverseEnd = System.nanoTime();
//        long birthTraverseTime = birthTraverseEnd - birthTraverseStart;
//        System.out.println("Traverse InputVector: " + birthTraverseTime + "ns");

        // Add missing birth bitsets to cache
//        long birthCachingStart = System.nanoTime();
        for (Map.Entry<Integer, BitSet> entry : toCacheBirthBitsets.entrySet()) {
          CacheKey cacheKey = new CacheKey(cubletFileName, this.schema.getActionFieldName(),
              chunk.getChunkID(), entry.getKey());
//          cacheManager.put(cacheKey, entry.getValue(), storageLevel);
          cacheManager.addToCacheBitsets(cacheKey, entry.getValue(), storageLevel);
          cachedBirthBitsets.put(entry.getKey(), entry.getValue());
        }
//        long birthCachingEnd = System.nanoTime();
//        long birthCachingTime = birthCachingEnd - birthCachingStart;
//        System.out.println("Caching: " + birthCachingTime + "ns");
//        totalGenerateTime += (birthCheckTime + birthTraverseTime + birthCachingTime);
      }

      // Generate missing age bitsets
//      long ageGenerateStart = System.nanoTime();
      for (Map.Entry<String, Set<Integer>> entry : ageIDSets.entrySet()) {
        Set<Integer> ageIDSet = entry.getValue();
        Map<Integer, BitSet> cachedAgeFieldBitsets = cachedAgeBitsets.get(entry.getKey());
        if (cachedAgeFieldBitsets.size() < ageIDSet.size()) {
          Map<Integer, BitSet> toCacheAgeBitsets = Maps.newLinkedHashMap();
          // Check missing birth localIDs
          for (int id : ageIDSet) {
            if (!cachedAgeFieldBitsets.containsKey(id)) {
              BitSet bitSet = new BitSet(chunk.getRecords());
              toCacheAgeBitsets.put(id, bitSet);
            }
          }

          // Traverse InputVector to generate missing age bitsets
          FieldRS ageField = loadField(chunk, entry.getKey());
          InputVector ageInput = ageField.getValueVector();
          int pos = 0;
          ageInput.skipTo(pos);
          while (ageInput.hasNext()) {
            int key = ageInput.next();
            if (toCacheAgeBitsets.containsKey(key)) {
              toCacheAgeBitsets.get(key).set(pos);
            }
            pos++;
          }

          // Add missing age bitsets to cache
          for (Map.Entry<Integer, BitSet> en : toCacheAgeBitsets.entrySet()) {
            CacheKey cacheKey = new CacheKey(cubletFileName, entry.getKey(), chunk.getChunkID(),
                en.getKey());
            cacheManager.addToCacheBitsets(cacheKey, en.getValue(), storageLevel);
            cachedAgeFieldBitsets.put(en.getKey(), en.getValue());
          }
          cachedAgeBitsets.put(entry.getKey(), cachedAgeFieldBitsets);
        }
      }
      long generateEnd = System.nanoTime();
      totalGenerateTime += (generateEnd - generateStart);

//      long ageGenerateEnd = System.nanoTime();
//      long ageGenerateTime = ageGenerateEnd - ageGenerateStart;
//      totalGenerateTime += ageGenerateTime;

      // 3. Filter: Search matched rows
//      long filterStart = System.nanoTime();
      long filterStart = System.nanoTime();
      Map<String, BitSet> fieldMatchedRows = Maps.newLinkedHashMap();
      for (Map.Entry<String, Map<Integer, BitSet>> entry : cachedAgeBitsets.entrySet()) {
        BitSet fieldBitset = new BitSet(chunk.getRecords());
        Map<Integer, BitSet> cachedAgeFieldBitsets = entry.getValue();
        for (Map.Entry<Integer, BitSet> en : cachedAgeFieldBitsets.entrySet()) {
          fieldBitset.or(en.getValue());
        }
        fieldMatchedRows.put(entry.getKey(), fieldBitset);
      }

      bv.set(0, chunk.getRecords());
      for (Map.Entry<String, BitSet> entry : fieldMatchedRows.entrySet()) {
        bv.and(entry.getValue());
      }
      long filterEnd = System.nanoTime();
      totalFilterTime += (filterEnd - filterStart);

      // Filter Cache
      Map<String, FieldFilter> ageFilters = this.sigma.getAgeFilters();
      for (Map.Entry<String, FieldFilter> entry : ageFilters.entrySet()) {
        if (entry.getValue() instanceof RangeFieldFilter) {
          // Proper range of filter range length
          // TODO: Need to get from Controller
          Range filterRangeLength = new Range(100, 10000);

          FieldFilter fieldFilter = entry.getValue();
          int[] minValues = ((RangeFieldFilter) fieldFilter).getMinValues();
          int[] maxValues = ((RangeFieldFilter) fieldFilter).getMaxValues();

          // 1. Load: Load cache
          // Construct cache keys
          // TODO: Suppose one filter one range first, need to deal with multiple ranges
//          long loadStart = System.nanoTime();
          List<CacheKey> cacheKeys = Lists.newArrayList();
          Range searchedRange = new Range(minValues[0], maxValues[0]);
          CacheKey searchedKey = new CacheKey(CacheKeyType.FILTER, cubletFileName, entry.getKey(),
              chunk.getChunkID(), searchedRange);
          cacheKeys.add(searchedKey);

          // Load cache
          Map<CacheKey, BitSet> cachedBitsets = cacheManager.load(cacheKeys, storageLevel);
//          long loadEnd = System.nanoTime();
//          totalLoadTime += (loadEnd - loadStart);

          // 2. Reuse Cases
          // Exact/Subsuming/Partial Reuse
          if (cachedBitsets.size() == 1) {
            for (Map.Entry<CacheKey, BitSet> en : cachedBitsets.entrySet()) {
              Range cachedRange = en.getKey().getRange();
              /* Can't directly use "BitSet cachedBitset = en.getValue();" !!!
                 Otherwise the bitset in cache will also be changed!
               */
              BitSet cachedBitset = new BitSet(chunk.getRecords());
              cachedBitset.or(en.getValue());
              // Exact Reuse
              if (searchedRange.compareTo(cachedRange) == RangeCase.EXACT) {
                System.out.println("Exact Reuse");
                bv.and(cachedBitset);
              }
              // Subsuming Reuse (searchedRange is smaller than cachedRange)
              else if (searchedRange.compareTo(cachedRange) == RangeCase.SUBSUMING) {
                System.out.println("Subsuming Reuse");

                // Traverse InputVector for further filtering
                FieldRS ageField = loadField(chunk, entry.getKey());
                InputVector ageInput = ageField.getValueVector();
                int off = 0;
                // Search in bit=1
                off = cachedBitset.nextSetBit(off);
                while (off < ageInput.size() && off >= 0) {
                  ageInput.skipTo(off);
                  if (!fieldFilter.accept(ageInput.next())) {
                    cachedBitset.clear(off);
                  }
                  off = cachedBitset.nextSetBit(off + 1);
                }

                // Get filter result
                bv.and(cachedBitset);

                // Caching filter bitset
                if (filterRangeLength.contains(searchedRange.getLength()) && !filterRangeLength
                    .contains(cachedRange.getLength())) {
                  cacheManager.remove(en.getKey(), storageLevel);
                  cacheManager.addToCacheBitsets(searchedKey, cachedBitset, storageLevel);
                }
              }
              // Partial Reuse (searchedRange is larger than cachedRange)
              else if (searchedRange.compareTo(cachedRange) == RangeCase.PARTIAL) {
                System.out.println("Partial Reuse");

                // Traverse InputVector to add qualified records
                FieldRS ageField = loadField(chunk, entry.getKey());
                InputVector ageInput = ageField.getValueVector();
                int off = 0;
                // Search in bit=0
                off = cachedBitset.nextClearBit(off);
                while (off < ageInput.size() && off >= 0) {
                  ageInput.skipTo(off);
                  if (fieldFilter.accept(ageInput.next())) {
                    cachedBitset.set(off);
                  }
                  off = cachedBitset.nextClearBit(off + 1);
                }

                // Get filter result
                bv.and(cachedBitset);

                // Caching filter bitset
                if (filterRangeLength.contains(searchedRange.getLength()) && !filterRangeLength
                    .contains(cachedRange.getLength())) {
                  cacheManager.remove(en.getKey(), storageLevel);
                  cacheManager.addToCacheBitsets(searchedKey, cachedBitset, storageLevel);
                }
              }
            }
          }
          // Partial Reuse (more than one candidate range)
          else if (cachedBitsets.size() > 1) {
            System.out.println("Multiple Partial Reuse");

            boolean allNotInFilterRangeLength = true;
            BitSet cachedBitset = new BitSet(chunk.getRecords());
            Range cachedRange = null;
            for (Map.Entry<CacheKey, BitSet> en : cachedBitsets.entrySet()) {
              if (cachedRange == null) {
                /* Can't directly use "cachedRange = entry.getKey().getRange();" !!!
                   Otherwise the range in cache will also be changed!
                 */
                cachedRange = new Range(en.getKey().getRange());
              } else if (cachedRange.compareTo(en.getKey().getRange()) != RangeCase.NOOVERLAP) {
                cachedRange.union(en.getKey().getRange());
              }
              cachedBitset.or(en.getValue());
              if (filterRangeLength.contains(en.getKey().getRange().getLength())) {
                allNotInFilterRangeLength = false;
              }
            }

            // Traverse InputVector to add qualified records
            if (!searchedRange.equals(cachedRange)) {
              FieldRS ageField = loadField(chunk, entry.getKey());
              InputVector ageInput = ageField.getValueVector();
              int off = 0;
              // Search in bit=0
              off = cachedBitset.nextClearBit(off);
              while (off < ageInput.size() && off >= 0) {
                ageInput.skipTo(off);
                if (fieldFilter.accept(ageInput.next())) {
                  cachedBitset.set(off);
                }
                off = cachedBitset.nextClearBit(off + 1);
              }
            }

            // Get filter result
            bv.and(cachedBitset);

            // Caching filter bitset
            if (filterRangeLength.contains(searchedRange.getLength()) && allNotInFilterRangeLength) {
              for (Map.Entry<CacheKey, BitSet> en : cachedBitsets.entrySet()) {
                cacheManager.remove(en.getKey(), storageLevel);
              }
              cacheManager.addToCacheBitsets(searchedKey, cachedBitset, storageLevel);
            }
          }
          // No Reuse
          else if (cachedBitsets.size() == 0) {
            System.out.println("No Reuse");

            BitSet filterBitSet = new BitSet(chunk.getRecords());
            FieldRS ageField = loadField(chunk, entry.getKey());
            InputVector ageInput = ageField.getValueVector();
            ageInput.skipTo(0);
            for (int i = 0; i < ageInput.size(); i++) {
              if (fieldFilter.accept(ageInput.next())) {
                filterBitSet.set(i);
              }
            }
            // Get filter result
            bv.and(filterBitSet);
            cacheManager.addToCacheBitsets(searchedKey, filterBitSet, storageLevel);
          }
        }
      }
    }

    while (appInput.hasNext()) {
      appInput.nextBlock(appBlock);
      if (appFilter.accept(appBlock.value)) {
        if (!(userField.getValueVector() instanceof RLEInputVector)) {
          continue;
        }
        RLEInputVector userInput = (RLEInputVector) userField.getValueVector();
        userInput.skipTo(0);
        RLEInputVector.Block userBlock = new RLEInputVector.Block();
        while (userInput.hasNext()) {
          userInput.nextBlock(userBlock);
          if (userBlock.off < appBlock.off) {
            continue;
          }
          if (userBlock.off > appBlock.off + appBlock.len) {
            break;
          }

          int begin = userBlock.off;
          int end = userBlock.off + userBlock.len;

          actionInput.skipTo(begin);

          int birthOff;

          long seekStart = System.nanoTime();
          if (reuse) {
            birthOff = seekToReuseBirthTuple(begin, end, cachedBirthBitsets);
          } else {
            birthOff = seekToBirthTuple(begin, end, actionInput);
          }
          long seekEnd = System.nanoTime();
          totalSeekTime += (seekEnd - seekStart);

          if (birthOff == end) {
            continue;
          }

          if (this.sigma.selectUser(birthOff)) {
            this.bs.set(begin, end);
            cohortInput.skipTo(birthOff);
            int cohort = cohortInput.next() - min;
            chunkResults[cohort][0]++;
            actionTimeInput.skipTo(birthOff);
            int birthTime = actionTimeInput.next();
            int ageOff = birthOff + 1;
            if (ageOff < end) {
              long selectionStart = System.nanoTime();
//              if (reuse) {
//                this.sigma.selectAgeActivitiesUsingBitset(birthOff + 1, end, bv);
//              } else {
//                bv.set(birthOff + 1, end);
//                this.sigma.selectAgeActivities(birthOff + 1, end, bv);
//              }
              if (!reuse) {
                bv.set(birthOff + 1, end);
                this.sigma.selectAgeActivities(birthOff + 1, end, bv);
              }
              long selectionEnd = System.nanoTime();
              totalSelectionTime += (selectionEnd - selectionStart);
              long aggregationStart = System.nanoTime();
              if (!bv.isEmpty()) {
                aggregator.processUser(bv, birthTime, birthOff + 1, end, chunkResults[cohort]);
              }
              long aggregationEnd = System.nanoTime();
              totalAggregationTime += (aggregationEnd - aggregationStart);
//              bv.clear(birthOff + 1, end);
              bv.clear(begin, end);
            }
          }
        }
      }
    }

    long aggregationStart = System.nanoTime();
    InputVector keyVector = null;
    if (cohortField.isSetField()) {
      keyVector = cohortField.getKeyVector();
    }
    for (int i = 0; i < cardinality; i++) {
      if (chunkResults[i][0] > 0) {
        int cohort = keyVector == null ? i + min : keyVector.get(i + min);
        for (int j = 0; j < cohortSize; j++) {
          CohortKey key = new CohortKey(cohort, j);
          long value = 0;
          if (this.cubletResults.containsKey(key)) {
            value = this.cubletResults.get(key);
          }
          if (value + chunkResults[i][j] > 0) {
            this.cubletResults.put(key, value + chunkResults[i][j]);
          }
        }
      }
    }
    long aggregationEnd = System.nanoTime();
    totalAggregationTime += (aggregationEnd - aggregationStart);
  }

  private synchronized FieldRS loadField(ChunkRS chunk, int fieldId) {
    return chunk.getField(fieldId);
  }

  private FieldRS loadField(ChunkRS chunk, String fieldName) {
    if ("Retention".equals(fieldName)) {
      return null;
    }
    int id = this.schema.getFieldID(fieldName);
    return loadField(chunk, id);
  }

  private Aggregator newAggregator() {
    String metric = this.query.getMetric();
    if (metric.equals("Retention")) {
      return new UserCountAggregator();
    } else {
      return new SumAggregator();
    }
  }

  private int seekToBirthTuple(int begin, int end, InputVector actionInput) {
    int pos = begin - 1;
    for (int id : this.bBirthActionChunkIDs) {
      pos++;
      for (; pos < end; pos++) {
        if (actionInput.next() == id) {
          break;
        }
      }
    }
    return Math.min(pos, end);
  }

  private int seekToReuseBirthTuple(int begin, int end, Map<Integer, BitSet> bitSetList) {
    int pos = begin - 1;

    for (int id : this.bBirthActionChunkIDs) {
      BitSet bitSet = bitSetList.get(id);
      int newPos = bitSet.nextSetBit(pos + 1);
      if (newPos < 0 || newPos >= end) {
        return end;
      }
      pos = newPos;
    }
    return pos;
  }
}
