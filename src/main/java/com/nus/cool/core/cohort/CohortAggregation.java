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

//<<<<<<< HEAD
import static com.google.common.base.Preconditions.checkNotNull;

//=======
import com.google.common.collect.Lists;
//>>>>>>> Add seekToReuseBirthTuple()
import com.google.common.collect.Maps;
import com.nus.cool.core.cohort.aggregator.Aggregator;
import com.nus.cool.core.cohort.aggregator.SumAggregator;
import com.nus.cool.core.cohort.aggregator.UserCountAggregator;
import com.nus.cool.core.cohort.filter.FieldFilter;
import com.nus.cool.core.io.Input;
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
//<<<<<<< HEAD
//=======
import java.io.IOException;
import java.util.LinkedHashMap;
import lombok.Getter;

//>>>>>>> Implement disk cache(load & put); Add Controller & Processor
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

  public CohortAggregation(CohortSelection sigma) {
    this.sigma = checkNotNull(sigma);
  }
//<<<<<<< HEAD
//
//  @Override
//  public void init(TableSchema schema, CohortQuery query) {
//    this.schema = checkNotNull(schema);
//    this.query = checkNotNull(query);
//    this.birthActions = query.getBirthActions();
//    this.sigma.init(schema, query);
//  }
//
//  @Override
//  public void process(MetaChunkRS metaChunk) {
//    this.sigma.process(metaChunk);
//
//    int actionField = this.schema.getActionField();
//    MetaFieldRS actionMetaField = metaChunk.getMetaField(actionField, FieldType.Action);
//    this.birthActionGlobalIDs = new int[this.birthActions.length];
//    for (int i = 0; i < this.birthActions.length; i++) {
//      int id = actionMetaField.find(this.birthActions[i]);
//        if (id < 0) {
//            throw new RuntimeException("Unknown birth action: " + this.birthActions[i]);
//        }
//      this.birthActionGlobalIDs[i] = id;
//    }
//  }
//
//  @Override
//  public void process(ChunkRS chunk) {
//    this.sigma.process(chunk);
//      if (!this.sigma.isBUserActiveChunk()) {
//          return;
//      }
//
//    FieldRS appField = loadField(chunk, this.schema.getAppKeyField());
//    FieldRS userField = loadField(chunk, this.schema.getUserKeyField());
//    FieldRS actionField = loadField(chunk, this.schema.getActionField());
//    FieldRS actionTimeField = loadField(chunk, this.schema.getActionTimeField());
//    FieldRS cohortField = loadField(chunk, this.query.getCohortFields()[0]);
//    FieldRS metricField = loadField(chunk, this.query.getMetric());
//    this.bBirthActionChunkIDs = new int[this.birthActionGlobalIDs.length];
//    for (int i = 0; i < this.birthActionGlobalIDs.length; i++) {
//      int id = actionField.getKeyVector().find(this.birthActionGlobalIDs[i]);
//        if (id < 0) {
//            return;
//        }
//      this.bBirthActionChunkIDs[i] = id;
//    }
//
////<<<<<<< HEAD
////    int min = cohortField.minKey();
////    int cardinality = cohortField.maxKey() - min + 1;
////    int cohortSize = actionTimeField.maxKey() - actionTimeField.minKey() + 1 + 1;
////    long[][] chunkResults = new long[cardinality][cohortSize];
////
////    InputVector cohortInput = cohortField.getValueVector();
////    InputVector actionTimeInput = actionTimeField.getValueVector();
////    InputVector metricInput = metricField == null ? null : metricField.getValueVector();
////    Aggregator aggregator = newAggregator();
////    int minAllowedAge = 0;
////    int maxAllowedAge = cohortSize - 1;
////    aggregator.init(metricInput, actionTimeInput, cohortSize, minAllowedAge, maxAllowedAge,
////        this.query.getAgeInterval());
////
////    RLEInputVector appInput = (RLEInputVector) appField.getValueVector();
////    appInput.skipTo(0);
////    RLEInputVector.Block appBlock = new RLEInputVector.Block();
////    FieldFilter appFilter = this.sigma.getAppFilter();
////    BitSet bv = new BitSet(chunk.getRecords());
////    this.bs = new BitSet(chunk.getRecords());
////
////    while (appInput.hasNext()) {
////      appInput.nextBlock(appBlock);
////      if (appFilter.accept(appBlock.value)) {
////          if (!(userField.getValueVector() instanceof RLEInputVector)) {
////              continue;
////          }
////        RLEInputVector userInput = (RLEInputVector) userField.getValueVector();
////        userInput.skipTo(0);
////        RLEInputVector.Block userBlock = new RLEInputVector.Block();
////        while (userInput.hasNext()) {
////          userInput.nextBlock(userBlock);
////            if (userBlock.off < appBlock.off) {
////                continue;
////            }
////            if (userBlock.off > appBlock.off + appBlock.len) {
////                break;
////=======
//        int min = cohortField.minKey();
//        int cardinality = cohortField.maxKey() - min + 1;
//        int cohortSize = actionTimeField.maxKey() - actionTimeField.minKey() + 1 + 1;
//        long[][] chunkResults = new long[cardinality][cohortSize];
//
//        InputVector cohortInput = cohortField.getValueVector();
//        InputVector actionTimeInput = actionTimeField.getValueVector();
//        InputVector metricInput = metricField == null ? null : metricField.getValueVector();
//        Aggregator aggregator = newAggregator();
//        int minAllowedAge = 0;
//        int maxAllowedAge = cohortSize - 1;
//        aggregator.init(metricInput, actionTimeInput, cohortSize, minAllowedAge, maxAllowedAge, this.query.getAgeInterval());
//
//        RLEInputVector appInput = (RLEInputVector) appField.getValueVector();
//        appInput.skipTo(0);
//        RLEInputVector.Block appBlock = new RLEInputVector.Block();
//        FieldFilter appFilter = this.sigma.getAppFilter();
//        BitSet bv = new BitSet(chunk.getRecords());
//        this.bs = new BitSet(chunk.getRecords());
//
//        while (appInput.hasNext()) {
//            appInput.nextBlock(appBlock);
//            if (appFilter.accept(appBlock.value)) {
//                if (!(userField.getValueVector() instanceof RLEInputVector))
//                    continue;
//                RLEInputVector userInput = (RLEInputVector) userField.getValueVector();
//                userInput.skipTo(0);
//                RLEInputVector.Block userBlock = new RLEInputVector.Block();
//                while (userInput.hasNext()) {
//                    userInput.nextBlock(userBlock);
//                    if (userBlock.off < appBlock.off)
//                        continue;
//                    if (userBlock.off > appBlock.off + appBlock.len)
//                        break;
//
//                    int begin = userBlock.off;
//                    int end = userBlock.off + userBlock.len;
//
//                    InputVector actionInput = actionField.getValueVector();
//                    actionInput.skipTo(begin);
//
//                    boolean reuse = true;
//                    int birthOff;
//
//                    if (reuse) {
//=======

  @Override
  public void init(TableSchema schema, CohortQuery query) {
    this.schema = checkNotNull(schema);
    this.query = checkNotNull(query);
    this.birthActions = query.getBirthActions();
    this.sigma.init(schema, query);
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
  public void process(ChunkRS chunk, CacheManager cacheManager, String cubletFileName)
      throws IOException {
    this.sigma.process(chunk, cacheManager, cubletFileName);
    if (!this.sigma.isBUserActiveChunk()) {
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

    // Load chunk cache
    List<CacheKey> cacheKeys = Lists.newArrayList();
    for (int i = 0; i < this.bBirthActionChunkIDs.length; i++) {
      CacheKey cacheKey = new CacheKey(cubletFileName, chunk.getChunkID(),
          this.bBirthActionChunkIDs[i]);
      cacheKeys.add(cacheKey);
    }
    Map<Integer, BitSet> cachedBitsets = cacheManager.load(cacheKeys);

    System.out.println(cachedBitsets.size());
    System.out.println(cachedBitsets);

    // Caching missing Bitsets
    if (cachedBitsets.size() < this.bBirthActionChunkIDs.length) {
      System.out.println("Caching missing Bitsets");
      LinkedHashMap<Integer, BitSet> toCacheBitsets = new LinkedHashMap<>();
      // Check missed localIDs
      for (int id : this.bBirthActionChunkIDs) {
        if (!cachedBitsets.containsKey(id)) {
          BitSet bitSet = new BitSet(chunk.getRecords());
          toCacheBitsets.put(id, bitSet);
          System.out.println("missing id: " + id);
        }
      }

      // Traverse actionInput to get missed Bitsets
      int pos = 0;
      actionInput.skipTo(pos);
      while (actionInput.hasNext()) {
        int key = actionInput.next();
        if (toCacheBitsets.containsKey(key)) {
          toCacheBitsets.get(key).set(pos);
        }
        pos++;
      }

      // Caching bitsets
      for (Map.Entry<Integer, BitSet> entry : toCacheBitsets.entrySet()) {
        CacheKey cacheKey = new CacheKey(cubletFileName, chunk.getChunkID(), entry.getKey());
        cacheManager.put(cacheKey, entry.getValue());
        cachedBitsets.put(entry.getKey(), entry.getValue());
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

//          InputVector actionInput = actionField.getValueVector();
          actionInput.skipTo(begin);

          boolean reuse = true;
          int birthOff;

          if (reuse) {
//>>>>>>> Implement disk cache(load & put); Add Controller & Processor
//                        List<BitSet> bitSetList = Lists.newArrayList();
//
//                        BitSet bitSet1 = new BitSet(10000);
//                        for(int i=20; i<10000; i+=1314)
//                            bitSet1.set(i);
//                        bitSetList.add(bitSet1);
//
//                        BitSet bitSet2 = new BitSet(10000);
//                        for(int i=55; i<10000; i+=523)
//                            bitSet2.set(i);
//                        bitSetList.add(bitSet2);
//
//                        BitSet bitSet3 = new BitSet(10000);
//                        for(int i=99; i<10000; i+=520)
//                            bitSet3.set(i);
//                        bitSetList.add(bitSet3);

//<<<<<<< HEAD
//                        List<BitSet> bitSetList = reuseAndCaching(actionInput);
//                        birthOff = seekToReuseBirthTuple(begin, end, bitSetList);
//
//                        System.out.println("chunk id: " + chunk.getChunkID());
//                        System.out.println("begin: " + begin);
//                        System.out.println("end: " + end);
//                        System.out.println("birthOff: " + birthOff);
//                        System.out.println();
//
//                    }
//                    else {
//                        birthOff = seekToBirthTuple(begin, end, actionInput);
//                    }
//
//                    if (birthOff == end) {
//                        continue;
//                    }
//
//                    if (this.sigma.selectUser(birthOff)) {
//                        this.bs.set(begin, end);
//                        cohortInput.skipTo(birthOff);
//                        int cohort = cohortInput.next() - min;
//                        chunkResults[cohort][0]++;
//                        actionTimeInput.skipTo(birthOff);
//                        int birthTime = actionTimeInput.next();
//                        int ageOff = birthOff + 1;
//                        if (ageOff < end) {
//                            bv.set(birthOff + 1, end);
//                            this.sigma.selectAgeActivities(birthOff + 1, end, bv);
//                            if (!bv.isEmpty())
//                                aggregator.processUser(bv, birthTime, birthOff + 1, end, chunkResults[cohort]);
//                            bv.clear(birthOff + 1, end);
//                        }
//                    }
//                }
////>>>>>>> Add seekToReuseBirthTuple()
//            }
//
////          int begin = userBlock.off;
////          int end = userBlock.off + userBlock.len;
////          InputVector actionInput = actionField.getValueVector();
////          actionInput.skipTo(begin);
////          int birthOff = seekToBirthTuple(begin, end, actionInput);
////          if (birthOff == end) {
////            continue;
////          }
////
////          if (this.sigma.selectUser(birthOff)) {
////            this.bs.set(begin, end);
////            cohortInput.skipTo(birthOff);
////            int cohort = cohortInput.next() - min;
////            chunkResults[cohort][0]++;
////            actionTimeInput.skipTo(birthOff);
////            int birthTime = actionTimeInput.next();
////            int ageOff = birthOff + 1;
////            if (ageOff < end) {
////              bv.set(birthOff + 1, end);
////              this.sigma.selectAgeActivities(birthOff + 1, end, bv);
////                if (!bv.isEmpty()) {
////                    aggregator.processUser(bv, birthTime, birthOff + 1, end, chunkResults[cohort]);
////                }
////              bv.clear(birthOff + 1, end);
////            }
////          }
////        }
//      }
////    }
//
//    InputVector keyVector = null;
//      if (cohortField.isSetField()) {
//          keyVector = cohortField.getKeyVector();
//      }
//=======
//            List<BitSet> bitSetList = reuseAndCaching(actionInput, cacheManager, cubletFileName, chunk.getChunkID());
            birthOff = seekToReuseBirthTuple(begin, end, cachedBitsets);

            System.out.println("chunk id: " + chunk.getChunkID());
            System.out.println("begin: " + begin);
            System.out.println("end: " + end);
            System.out.println("birthOff: " + birthOff);
            System.out.println();

          } else {
            birthOff = seekToBirthTuple(begin, end, actionInput);
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
              this.sigma.selectAgeActivities(birthOff + 1, end, bv);
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
//>>>>>>> Implement disk cache(load & put); Add Controller & Processor
    for (int i = 0; i < cardinality; i++) {
      if (chunkResults[i][0] > 0) {
        int cohort = keyVector == null ? i + min : keyVector.get(i + min);
        for (int j = 0; j < cohortSize; j++) {
          CohortKey key = new CohortKey(cohort, j);
          long value = 0;
//<<<<<<< HEAD
//            if (this.cubletResults.containsKey(key)) {
//                value = this.cubletResults.get(key);
//            }
//            if (value + chunkResults[i][j] > 0) {
//                this.cubletResults.put(key, value + chunkResults[i][j]);
//            }
//        }
//      }
//    }
//  }
//
//  private synchronized FieldRS loadField(ChunkRS chunk, int fieldId) {
//    return chunk.getField(fieldId);
//  }
//
//  private FieldRS loadField(ChunkRS chunk, String fieldName) {
//      if ("Retention".equals(fieldName)) {
//          return null;
//      }
//    int id = this.schema.getFieldID(fieldName);
//    return loadField(chunk, id);
//  }
//
//  private Aggregator newAggregator() {
//    String metric = this.query.getMetric();
//      if (metric.equals("Retention")) {
//          return new UserCountAggregator();
//      } else {
//          return new SumAggregator();
//      }
//  }
//
//  private int seekToBirthTuple(int begin, int end, InputVector actionInput) {
//    int pos = begin - 1;
//    for (int id : this.bBirthActionChunkIDs) {
//      pos++;
//      for (; pos < end; pos++) {
//        if (actionInput.next() == id) {
//          break;
//        }
//      }
//    }
////<<<<<<< HEAD
//    return Math.min(pos, end);
//  }
////=======
//=======
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
//>>>>>>> Implement disk cache(load & put); Add Controller & Processor

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

//  private List<BitSet> reuseAndCaching(InputVector actionInput, CacheManager cacheManager,
//      String cubletFileName, int chunkID) throws IOException {
//    Map<Integer, BitSet> cachedBitsets = Maps.newTreeMap();
//    Map<Integer, BitSet> toCacheBitsets = Maps.newTreeMap();
//
//    // Load cached bitsets
////        BitSet bitSet1 = new BitSet(actionInput.size());
////        for(int i=20; i<actionInput.size(); i+=1314)
////            bitSet1.set(i);
////        cachedBitsets.put(this.bBirthActionChunkIDs[0], bitSet1);
//
//    // Check missed localIDs
//    for (int id : this.bBirthActionChunkIDs) {
//      if (!cachedBitsets.containsKey(id)) {
//        BitSet bitSet = new BitSet(actionInput.size());
//        toCacheBitsets.put(id, bitSet);
//      }
//    }
//
//    // Traverse actionInput to get missed bitsets
//    int pos = 0;
//    actionInput.skipTo(pos);
//    while (actionInput.hasNext()) {
//      int key = actionInput.next();
//      if (toCacheBitsets.containsKey(key)) {
//        toCacheBitsets.get(key).set(pos);
//      }
//      pos++;
//    }
//
//    // Caching bitsets
//    for (Map.Entry<Integer, BitSet> entry : toCacheBitsets.entrySet()) {
//      System.out.println(cubletFileName);
//      CacheKey cacheKey = new CacheKey(cubletFileName, chunkID, entry.getKey());
//      cacheManager.put(cacheKey, entry.getValue());
//    }
//
//    // Merge BitSets
//    List<BitSet> bitSetList = Lists.newArrayList();
//    for (int id : this.bBirthActionChunkIDs) {
//      if (cachedBitsets.containsKey(id)) {
//        bitSetList.add(cachedBitsets.get(id));
//      } else if (toCacheBitsets.containsKey(id)) {
//        bitSetList.add(toCacheBitsets.get(id));
//      } else {
//        System.out.println("Bitset of localID " + id + " is not found!");
//      }
//    }
//    return bitSetList;
//  }

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
//<<<<<<< HEAD
////>>>>>>> Add seekToReuseBirthTuple()
//=======
    return pos;
  }
//>>>>>>> Implement disk cache(load & put); Add Controller & Processor
}
