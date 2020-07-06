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
import com.nus.cool.core.io.cache.CacheKey;
import com.nus.cool.core.io.cache.CacheManager;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.io.storevector.RLEInputVector;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import lombok.Getter;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import lombok.Getter;

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
  public double totalCachingTime;

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
    totalCachingTime = 0;
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

//    System.out.println("*** Chunk ID: " + chunk.getChunkID() + " ***");
    chunkNum++;

    // 1. Load: Load chunk cache
    Set<Integer> birthIDSet = new HashSet<>();
    Map<Integer, BitSet> cachedBirthBitsets = Maps.newLinkedHashMap();
    Map<String, Set<Integer>> ageIDSets = Maps.newLinkedHashMap();
    Map<String, Map<Integer, BitSet>> cachedAgeBitsets = Maps.newLinkedHashMap();
    BitSet ageMatchedRows = new BitSet(chunk.getRecords());

    if (reuse) {
      // Construct birth action cacheKeys
      long constructStart = System.nanoTime();
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
      long constructEnd = System.nanoTime();
      long constructTime = constructEnd - constructStart;
//      System.out.println("Construct CacheKeys: " + constructTime + "ns");

//      System.out.println("*** Loading cached Bitsets ***");
      // Load birth action cache
      long loadStart = System.nanoTime();
      cachedBirthBitsets = cacheManager.load(birthCacheKeys, storageLevel);
      long loadEnd = System.nanoTime();
      long loadTime = (loadEnd - loadStart);
//      System.out.println("Load chunk cache: " + loadTime + "ns");
      totalLoadTime += (constructTime + loadTime);

//      System.out.println("Load " + cachedBirthBitsets.size() + " Bitsets");
//      if (!cachedBirthBitsets.isEmpty()) {
//        System.out.println("Local IDs:");
//        for (Map.Entry<Integer, BitSet> entry : cachedBirthBitsets.entrySet()) {
//          System.out.println(entry.getKey());
//        }
//      }

      // Construct age selection cacheKeys
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
        Map<Integer, BitSet> cachedAgeFieldBitsets = cacheManager.load(ageCacheKeys, storageLevel);
        cachedAgeBitsets.put(entry.getKey(), cachedAgeFieldBitsets);
      }

      // 2. Generate: Generate missing bitsets
      // Generate missing birth bitsets
      if (cachedBirthBitsets.size() < birthIDSet.size()) {
//        System.out.println("*** Caching missing Bitsets ***");
        long checkStart = System.nanoTime();
        Map<Integer, BitSet> toCacheBirthBitsets = Maps.newLinkedHashMap();
        // Check missing birth localIDs
        for (int id : birthIDSet) {
          if (!cachedBirthBitsets.containsKey(id)) {
            BitSet bitSet = new BitSet(chunk.getRecords());
            toCacheBirthBitsets.put(id, bitSet);
//            System.out.println("Missing local ID: " + id);
          }
        }
        long checkEnd = System.nanoTime();
        long checkTime = checkEnd - checkStart;
//        System.out.println("Check missed localIDs: " + checkTime + "ns");

        // Traverse actionInput to generate missing birth bitsets
        long traverseStart = System.nanoTime();
        int pos = 0;
        actionInput.skipTo(pos);
        while (actionInput.hasNext()) {
          int key = actionInput.next();
          if (toCacheBirthBitsets.containsKey(key)) {
            toCacheBirthBitsets.get(key).set(pos);
          }
          pos++;
        }
        long traverseEnd = System.nanoTime();
        long traverseTime = traverseEnd - traverseStart;
//        System.out.println("Traverse InputVector: " + traverseTime + "ns");

        // Add missing birth bitsets to cache
        long cachingStart = System.nanoTime();
        for (Map.Entry<Integer, BitSet> entry : toCacheBirthBitsets.entrySet()) {
          CacheKey cacheKey = new CacheKey(cubletFileName, this.schema.getActionFieldName(),
              chunk.getChunkID(), entry.getKey());
//          cacheManager.put(cacheKey, entry.getValue(), storageLevel);
          cacheManager.addToCacheBitsets(cacheKey, entry.getValue(), storageLevel);
          cachedBirthBitsets.put(entry.getKey(), entry.getValue());
        }
        long cachingEnd = System.nanoTime();
        long cachingTime = cachingEnd - cachingStart;
//        System.out.println("Caching: " + cachingTime + "ns");
        totalCachingTime += (checkTime + traverseTime + cachingTime);
      }

      // Generate missing age bitsets
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

      // 3. Filter: Search matched rows
      Map<String, BitSet> fieldMatchedRows = Maps.newLinkedHashMap();
      for (Map.Entry<String, Map<Integer, BitSet>> entry : cachedAgeBitsets.entrySet()) {
        BitSet fieldBitset = new BitSet(chunk.getRecords());
        Map<Integer, BitSet> cachedAgeFieldBitsets = entry.getValue();
        for (Map.Entry<Integer, BitSet> en : cachedAgeFieldBitsets.entrySet()) {
          fieldBitset.or(en.getValue());
        }
        fieldMatchedRows.put(entry.getKey(), fieldBitset);
      }

      ageMatchedRows.set(0, chunk.getRecords());
      for (Map.Entry<String, BitSet> entry : fieldMatchedRows.entrySet()) {
        ageMatchedRows.and(entry.getValue());
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

          if (reuse) {
            long seekStart = System.nanoTime();
            birthOff = seekToReuseBirthTuple(begin, end, cachedBirthBitsets);
            long seekEnd = System.nanoTime();
//            System.out.println("seekToReuseBirthTuple: " + (endTime - startTime) + "ns");
            totalSeekTime += (seekEnd - seekStart);

//            System.out.println("begin: " + begin);
//            System.out.println("end: " + end);
//            System.out.println("birthOff: " + birthOff);
//            System.out.println();
          } else {
            long seekStart = System.nanoTime();
            birthOff = seekToBirthTuple(begin, end, actionInput);
            long seekEnd = System.nanoTime();
//            System.out.println("seekToBirthTuple: " + (endTime - startTime) + "ns");
            totalSeekTime += (seekEnd - seekStart);
          }

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
              bv.set(birthOff + 1, end);
              if (reuse) {
                bv.and(ageMatchedRows);
                this.sigma.selectAgeActivitiesUsingBitset(birthOff + 1, end, bv);
              }
              else {
                this.sigma.selectAgeActivities(birthOff + 1, end, bv);
              }
              if (!bv.isEmpty()) {
                aggregator.processUser(bv, birthTime, birthOff + 1, end, chunkResults[cohort]);
              }
              bv.clear(birthOff + 1, end);
            }
          }
        }
      }
    }

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
    int pos = begin;

    for (int id : this.bBirthActionChunkIDs) {
      BitSet bitSet = bitSetList.get(id);
      int newPos = bitSet.nextSetBit(pos);
      if (newPos == -1 || newPos >= end) {
        return end;
      }
      pos = newPos;
    }
    return pos;
  }
}
