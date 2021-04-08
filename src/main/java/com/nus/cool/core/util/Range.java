package com.nus.cool.core.util;

import static com.google.common.base.Preconditions.checkArgument;

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

  public int compareTo(Range range) {
    // null: -2
    if (range == null)
      return -2;
    // exact: 0
    if (min == range.getMin() && max == range.getMax())
      return 0;
    // less than (subsuming): -1
    if (min >= range.getMin() && max <= range.getMax())
      return -1;
    // larger than (partial): 1
    if (min <= range.getMin() && max >= range.getMax())
      return 1;
    // overlap: 2
    return 2;
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
