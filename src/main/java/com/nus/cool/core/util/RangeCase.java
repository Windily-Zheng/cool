package com.nus.cool.core.util;

public enum RangeCase {
  /**
   * this.min == range.min && this.max == range.max
   */
  EXACT,

  /**
   * this.min >= range.min && this.max <= range.max (less than)
   */
  SUBSUMING,

  /**
   * this.min <= range.min && this.max >= range.max (larger than)
   */
  PARTIAL,

  /**
   * this.max < range.min || this.min > range.max
   */
  NOOVERLAP,

  /**
   * other cases
   */
  OVERLAP
}
