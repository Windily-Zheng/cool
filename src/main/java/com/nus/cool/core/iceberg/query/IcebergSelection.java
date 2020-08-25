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
package com.nus.cool.core.iceberg.query;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nus.cool.core.cohort.filter.FieldFilter;
import com.nus.cool.core.cohort.filter.FieldFilterFactory;
import com.nus.cool.core.cohort.filter.SetFieldFilter;
import com.nus.cool.core.io.cache.CacheKey;
import com.nus.cool.core.io.cache.CacheManager;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.converter.DayIntConverter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.swing.plaf.synth.SynthEditorPaneUI;

/**
 * @author hongbin
 * @version 0.1
 * @since 0.1
 */
public class IcebergSelection {

  private boolean bActivateCublet;

  private TableSchema tableSchema;

  private SelectionFilter filter;

  private IcebergQuery.granularityType granularity;

  private MetaChunkRS metaChunk;

  private String timeRange;

  private int max;

  private int min;

  private List<String> timeRanges;

  private List<Integer> maxs;

  private List<Integer> mins;

  // for testing
  public double totalLoadTime;

  // for testing
  public double totalGenerateTime;

  // for testing
  public double totalFilterTime;

  // for testing
  public double totalSelectionTime;

  private void splitTimeRange() throws ParseException {
    // TODO: format time range
    this.timeRanges = new ArrayList<>();
    this.maxs = new ArrayList<>();
    this.mins = new ArrayList<>();
    DayIntConverter converter = new DayIntConverter();
    switch (granularity) {
      case DAY:
        for (int i = 0; i < this.max - this.min; i++) {
          int time = this.min + i;
          this.maxs.add(time);
          this.mins.add(time);
          this.timeRanges.add(converter.getString(time));
        }
        break;
      case MONTH: {
        String[] timePoints = this.timeRange.split("\\|");
        Date d1 = new SimpleDateFormat("yyyy-MM").parse(timePoints[0]);
        Date d2 = new SimpleDateFormat("yyyy-MM").parse(timePoints[1]);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d2);
        calendar.add(Calendar.MONTH, 2);
        d2 = calendar.getTime();
        calendar.setTime(d1);
        List<String> points = new ArrayList<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        while (calendar.getTime().before(d2)) {
          points.add(sdf.format(calendar.getTime()));
          calendar.add(Calendar.MONTH, 1);
        }
        for (int i = 0; i < points.size() - 1; i++) {
          this.timeRanges.add(points.get(i) + "|" + points.get(i + 1));
          this.mins.add(converter.toInt(points.get(i)));
          this.maxs.add(converter.toInt(points.get(i + 1)) - 1);
        }
        this.mins.set(0, this.min);
        this.maxs.set(this.maxs.size() - 1, this.max);

        break;
      }
      case YEAR: {
        String[] timePoints = this.timeRange.split("\\|");
        Date d1 = new SimpleDateFormat("yyyy").parse(timePoints[0]);
        Date d2 = new SimpleDateFormat("yyyy").parse(timePoints[1]);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d2);
        calendar.add(Calendar.YEAR, 2);
        d2 = calendar.getTime();
        calendar.setTime(d1);
        List<String> points = new ArrayList<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        while (calendar.getTime().before(d2)) {
          points.add(sdf.format(calendar.getTime()));
          calendar.add(Calendar.YEAR, 1);
        }
        for (int i = 0; i < points.size() - 1; i++) {
          this.timeRanges.add(points.get(i) + "|" + points.get(i + 1));
          this.mins.add(converter.toInt(points.get(i)));
          this.maxs.add(converter.toInt(points.get(i + 1)));
        }
        break;
      }
      case NULL:
        this.maxs.add(this.max);
        this.mins.add(this.min);
        this.timeRanges.add(this.timeRange);
        break;
      default:
        throw new IllegalArgumentException();
    }
  }

  private void selectFields(BitSet bs, FieldRS field, FieldFilter filter) {
    InputVector fieldIn = field.getValueVector();
    int off = 0;
    while (off < fieldIn.size() && off >= 0) {
      fieldIn.skipTo(off);
      if (!filter.accept(fieldIn.next())) {
        bs.clear(off);
      }
      off = bs.nextSetBit(off + 1);
    }
  }

  private SelectionFilter init(SelectionQuery selection) {
    if (selection == null) {
      return null;
    }
    SelectionFilter filter = new SelectionFilter();
    filter.setType(selection.getType());
    if (filter.getType().equals(SelectionQuery.SelectionType.filter)) {
      FieldFilter fieldFilter = FieldFilterFactory.create(
          this.tableSchema.getField(selection.getDimension()), selection.getValues());
      filter.setFilter(fieldFilter);
      filter.setDimension(selection.getDimension());
    } else {
      for (SelectionQuery childSelection : selection.getFields()) {
        SelectionFilter childFilter = init(childSelection);
        filter.getFields().add(childFilter);
      }
    }
    return filter;
  }

  private boolean process(SelectionFilter selectionFilter, MetaChunkRS metaChunk) {
    if (selectionFilter == null) {
      return true;
    }
    if (selectionFilter.getType().equals(SelectionQuery.SelectionType.filter)) {
      MetaFieldRS metaField = metaChunk.getMetaField(selectionFilter.getDimension());
      return selectionFilter.getFilter().accept(metaField);
    } else {
      boolean flag = selectionFilter.getType().equals(SelectionQuery.SelectionType.and);
      for (SelectionFilter childFilter : selectionFilter.getFields()) {
        if (selectionFilter.getType().equals(SelectionQuery.SelectionType.and)) {
          flag &= process(childFilter, metaChunk);
        } else {
          flag |= process(childFilter, metaChunk);
        }
      }
      return flag;
    }
  }

  private boolean process(SelectionFilter selectionFilter, ChunkRS chunk) {
    if (selectionFilter == null) {
      return true;
    }
    if (selectionFilter.getType().equals(SelectionQuery.SelectionType.filter)) {
      FieldRS field = chunk.getField(selectionFilter.getDimension());
      return selectionFilter.getFilter().accept(field);
    } else {
      boolean flag = selectionFilter.getType().equals(SelectionQuery.SelectionType.and);
      for (SelectionFilter childFilter : selectionFilter.getFields()) {
        if (selectionFilter.getType().equals(SelectionQuery.SelectionType.and)) {
          flag &= process(childFilter, chunk);
        } else {
          flag |= process(childFilter, chunk);
        }
      }
      return flag;
    }
  }

  private BitSet select(SelectionFilter selectionFilter, ChunkRS chunk, BitSet bv, boolean reuse,
      CacheManager cacheManager, String storageLevel, String cubletFileName) throws IOException {
    BitSet bs = (BitSet) bv.clone();
    if (selectionFilter == null) {
      return bs;
    }
    if (selectionFilter.getType().equals(SelectionQuery.SelectionType.filter)) {
      String dimension = selectionFilter.getDimension();
      FieldRS field = chunk.getField(this.tableSchema.getFieldID(dimension));
      InputVector keyVector = field.getKeyVector();
      BitSet[] bitSets = field.getBitSets();
      boolean isPreCal = this.tableSchema.getField(dimension).isPreCal();
      if (reuse && field.isSetField() && isPreCal) {
        FieldFilter fieldFilter = selectionFilter.getFilter();
        BitSet filterBitset = ((SetFieldFilter) fieldFilter).getFilter();

        // 1. Load: Load cache
        // Construct cache keys
        long loadStart = System.nanoTime();
        List<CacheKey> cacheKeys = Lists.newArrayList();
        Set<Integer> localIDSet = new HashSet<>();
        int pos = 0;
        pos = filterBitset.nextSetBit(pos);
        while (pos >= 0) {
          localIDSet.add(pos);
          CacheKey cacheKey = new CacheKey(cubletFileName, dimension, chunk.getChunkID(), pos);
          cacheKeys.add(cacheKey);
          pos = filterBitset.nextSetBit(pos + 1);
        }

        // Load cache
        Map<Integer, BitSet> cachedBitsets = Maps.newLinkedHashMap();
        Map<CacheKey, BitSet> loadBitsets = cacheManager.load(cacheKeys, storageLevel);
        for (Map.Entry<CacheKey, BitSet> entry : loadBitsets.entrySet()) {
          cachedBitsets.put(entry.getKey().getLocalID(), entry.getValue());
        }
        long loadEnd = System.nanoTime();
        totalLoadTime += (loadEnd - loadStart);

        // 2. Generate: Generate missing bitsets
        if (cachedBitsets.size() < localIDSet.size()) {
          long generateStart = System.nanoTime();
          Map<Integer, BitSet> toCacheBitsets = Maps.newLinkedHashMap();
          // Check missing localIDs
          for (int id : localIDSet) {
            if (!cachedBitsets.containsKey(id)) {
              BitSet bitSet = new BitSet(chunk.getRecords());
              toCacheBitsets.put(id, bitSet);
            }
          }
          // Traverse InputVector to generate missing bitsets
          InputVector fieldIn = field.getValueVector();
          int off = 0;
          fieldIn.skipTo(off);
          while (fieldIn.hasNext()) {
            int key = fieldIn.next();
            if (toCacheBitsets.containsKey(key)) {
              toCacheBitsets.get(key).set(off);
            }
            off++;
          }
          // Add missing bitsets to cache
          for (Map.Entry<Integer, BitSet> entry : toCacheBitsets.entrySet()) {
            CacheKey cacheKey = new CacheKey(cubletFileName, dimension, chunk.getChunkID(),
                entry.getKey());
            cacheManager.addToCacheBitsets(cacheKey, entry.getValue(), storageLevel);
            cachedBitsets.put(entry.getKey(), entry.getValue());
          }
          long generateEnd = System.nanoTime();
          totalGenerateTime += (generateEnd - generateStart);
        }

        // 3. Filter: Search matched rows
        long filterStart = System.nanoTime();
        BitSet fieldBitset = new BitSet(chunk.getRecords());
        for (Map.Entry<Integer, BitSet> entry : cachedBitsets.entrySet()) {
          fieldBitset.or(entry.getValue());
        }
        bs.and(fieldBitset);
        long filterEnd = System.nanoTime();
        totalFilterTime += (filterEnd - filterStart);

//        List<String> values = selectionFilter.getFilter().getValues();
//        for (String value : values) {
//          int gId = this.metaChunk.getMetaField(selectionFilter.getDimension()).find(value);
//          int localId = keyVector.find(gId);
//          bs.or(bitSets[localId]);
//        }
      } else {
        long filterStart = System.nanoTime();
        selectFields(bs, field, selectionFilter.getFilter());
        long filterEnd = System.nanoTime();
        totalFilterTime += (filterEnd - filterStart);
      }
    } else if (selectionFilter.getType().equals(SelectionQuery.SelectionType.and)) {
      for (SelectionFilter childFilter : selectionFilter.getFields()) {
        bs = select(childFilter, chunk, bs, reuse, cacheManager, storageLevel, cubletFileName);
      }
    } else if (selectionFilter.getType().equals(SelectionQuery.SelectionType.or)) {
      List<BitSet> bitSets = new ArrayList<>();
      for (SelectionFilter childFilter : selectionFilter.getFields()) {
        bitSets
            .add(select(childFilter, chunk, bs, reuse, cacheManager, storageLevel, cubletFileName));
      }
      bs = orBitSets(bitSets);
    }
    return bs;
  }

  private BitSet orBitSets(List<BitSet> bitSets) {
    BitSet bs = bitSets.get(0);
    for (int i = 1; i < bitSets.size(); i++) {
      bs.or(bitSets.get(i));
    }
    return bs;
  }

  private boolean accept(int min, int max) {
    if (this.timeRange == null) {
      return true;
    }
    return (min <= this.max) && (max >= this.min);
  }

  public void init(TableSchema tableSchema, IcebergQuery query) throws ParseException {
    this.tableSchema = checkNotNull(tableSchema);
    checkNotNull(query);

    SelectionQuery selection = query.getSelection();
    this.filter = init(selection);

    if (query.getTimeRange() != null) {
      this.timeRange = query.getTimeRange();
      this.granularity = query.getGranularity();
      String[] timePoints = this.timeRange.split("\\|");
      DayIntConverter converter = new DayIntConverter();
      this.min = converter.toInt(timePoints[0]);
      this.max = converter.toInt(timePoints[1]);
      splitTimeRange();

      // for testing
      totalLoadTime = 0;
      totalGenerateTime = 0;
      totalFilterTime = 0;
      totalSelectionTime = 0;
    }
  }

  public void process(MetaChunkRS metaChunk) {
    this.metaChunk = metaChunk;
    MetaFieldRS timeField = metaChunk
        .getMetaField(this.tableSchema.getActionTimeField(), FieldType.ActionTime);
    this.bActivateCublet = accept(timeField.getMinValue(), timeField.getMaxValue())
        && process(this.filter, metaChunk);
  }

  public Map<String, BitSet> process(ChunkRS chunk, BitSet bitSet, boolean reuse,
      CacheManager cacheManager, String storageLevel, String cubletFileName) throws IOException {
//    System.out.println("cardinality: " + bitSet.cardinality());
    FieldRS timeField = chunk.getField(this.tableSchema.getActionTimeField());
    int minKey = timeField.minKey();
    int maxKey = timeField.maxKey();
    if (!(accept(minKey, maxKey) && process(this.filter, chunk))) {
      return null;
    }
    Map<String, BitSet> map = new HashMap<>();
    if (this.timeRange == null) {
      map.put("no time filter", bitSet);
    } else {
      int tag = 0;
      while (minKey > this.maxs.get(tag)) {
        tag += 1;
      }
      if (minKey >= this.mins.get(tag) & maxKey <= this.maxs.get(tag)) {
        BitSet bv = new BitSet(chunk.getRecords());
        bv.set(0, chunk.getRecords());
        map.put(this.timeRanges.get(tag), bv);
      } else {
        InputVector timeInput = timeField.getValueVector();
        timeInput.skipTo(0);
        BitSet[] bitSets = new BitSet[this.timeRanges.size()];
        for (int i = 0; i < this.timeRanges.size(); i++) {
          bitSets[i] = new BitSet(chunk.getRecords());
        }
        int min = this.mins.get(tag);
        int max = this.maxs.get(tag);
        for (int i = 0; i < timeInput.size(); i++) {
          int time = timeInput.next();
          if (time < this.min) {
            continue;
          }
          if (time > this.max) {
            break;
          }
          if (time >= min && time <= max) {
            bitSets[tag].set(i);
          } else {
            while (!(time >= this.mins.get(tag) && (time <= this.maxs.get(tag)))) {
              tag += 1;
            }
            min = this.mins.get(tag);
            max = this.maxs.get(tag);
            bitSets[tag].set(i);
          }
        }
        for (int i = 0; i < bitSets.length; i++) {
          if (bitSets[i].cardinality() != 0) {
            map.put(this.timeRanges.get(i), bitSets[i]);
          }
        }
      }
    }
    for (Map.Entry<String, BitSet> entry : map.entrySet()) {
      long selectionStart = System.nanoTime();
      BitSet bs = select(this.filter, chunk, entry.getValue(), reuse, cacheManager, storageLevel,
          cubletFileName);
      long selectionEnd = System.nanoTime();
      totalSelectionTime += (selectionEnd - selectionStart);
      map.put(entry.getKey(), bs);
    }
    return map;
  }

  public boolean isbActivateCublet() {
    return this.bActivateCublet;
  }
}
