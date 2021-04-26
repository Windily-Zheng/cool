package com.nus.cool.core.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import lombok.Getter;

public class Range {

  @Getter
  private int min;

  @Getter
  private int max;

  public Range(int min, int max) {
    this.min = min;
    this.max = max;
    checkArgument(this.min <= this.max);
  }

  public Range(String range) {
    String[] s = range.split("\\|");
    this.min = Integer.parseInt(s[0]);
    this.max = Integer.parseInt(s[1]);
    checkArgument(this.min <= this.max);
  }

  public Range(Range range) {
    checkNotNull(range);
    this.min = range.getMin();
    this.max = range.getMax();
    checkArgument(this.min <= this.max);
  }

  public RangeCase compareTo(Range range) {
    checkNotNull(range);
    // exact: 0
    if (min == range.getMin() && max == range.getMax()) {
      return RangeCase.EXACT;
    }
    // less than (subsuming): -1
    if (min >= range.getMin() && max <= range.getMax()) {
      return RangeCase.SUBSUMING;
    }
    // larger than (partial): 1
    if (min <= range.getMin() && max >= range.getMax()) {
      return RangeCase.PARTIAL;
    }
    // no overlap: 2
    if (range.getMax() < min || range.getMin() > max) {
      return RangeCase.NOOVERLAP;
    }
    // overlap: 3
    return RangeCase.OVERLAP;
  }

  public int getLength() {
    return this.max - this.min;
  }

  public boolean contains(int value) {
    return value >= this.min && value <= this.max;
  }

  public void union(Range range) {
    checkNotNull(range);
    // No overlap => Can't union
    if (this.compareTo(range) != RangeCase.NOOVERLAP) {
      if (range.getMin() < this.min) {
        this.min = range.getMin();
      }
      if (range.getMax() > this.max) {
        this.max = range.getMax();
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Range)) {
      return false;
    }
    Range range = (Range) o;
    return range.getMin() == this.min && range.getMax() == this.max;
//    return this.compareTo(range) == 0 || this.compareTo(range) == -1 || this.compareTo(range) == 1;
  }

  @Override
  public String toString() {
    return this.min + "|" + this.max;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + min;
    result = 31 * result + max;
    return result;
  }
}
