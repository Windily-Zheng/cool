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
package com.nus.cool.core.cohort.filter;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Lists;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.util.ArrayUtil;
import com.nus.cool.core.util.converter.NumericConverter;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.parser.VerticalTupleParser;
import java.util.List;
import lombok.Getter;

/**
 * Range Field Filter
 *
 * @author zhongle, hongbin
 * @version 0.1
 * @since 0.1
 */
public class RangeFieldFilter implements FieldFilter {

  @Getter
  private int[] minValues;

  @Getter
  private int[] maxValues;

  private int min;

  private int max;

  public RangeFieldFilter(List<String> values, NumericConverter converter) {
    checkNotNull(values);
    checkArgument(!values.isEmpty());
    this.minValues = new int[values.size()];
    this.maxValues = new int[values.size()];

    TupleParser parser = new VerticalTupleParser();
    for (int i = 0; i < values.size(); i++) {
      String[] range = parser.parse(values.get(i));
      this.minValues[i] = converter.toInt(range[0]);
      this.maxValues[i] = converter.toInt(range[1]);
      checkArgument(this.minValues[i] <= this.maxValues[i]);
    }
    this.min = ArrayUtil.min(this.minValues);
    this.max = ArrayUtil.max(this.maxValues);
  }

  @Override

  public int getMinKey() {
    return this.min;
  }

  @Override
  public int getMaxKey() {
    return this.max;
  }

  @Override
  public boolean accept(MetaFieldRS metaField) {
    return !(metaField.getMinValue() > this.max || metaField.getMaxValue() < this.min);
  }

  @Override
  public boolean accept(FieldRS field) {
    return !(field.minKey() > this.max || field.maxKey() < this.min);
  }

  @Override
  public boolean accept(int v) {
    boolean r = false;
    int i = 0;
    while (!r && i < this.minValues.length) {
      r = (v >= this.minValues[i] && v <= this.maxValues[i]);
      i++;
    }
    return r;
  }

  @Override
  public List<String> getValues() {
    List<String> values = Lists.newArrayList();
    for (int i = 0; i < this.minValues.length; i++) {
      values.add(this.minValues[i] + "|" + this.maxValues[i]);
    }
    return values;
  }
}
