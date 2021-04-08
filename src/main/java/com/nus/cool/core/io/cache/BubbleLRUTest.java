package com.nus.cool.core.io.cache;

import com.nus.cool.core.io.cache.utils.BubbleLRULinkedHashMap;
import com.nus.cool.core.util.Range;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.SortedSet;
import java.util.TreeSet;

public class BubbleLRUTest {

  public static void main(String[] args) {
//    LinkedHashMap<Integer, Integer> map = new LinkedHashMap<>(16, 0.75f, true);
//    map.put(1,1);
//    map.put(2,2);
//    map.put(3,3);
//    map.put(4,4);
//
//    map.entrySet().forEach(a->System.out.println(a));
//    System.out.println();
//
//    map.get(2);
//
//    map.entrySet().forEach(a->System.out.println(a));
//    System.out.println();
//
//    BubbleLRULinkedHashMap<Integer, Integer> map2 = new BubbleLRULinkedHashMap<>(16, 0.75f, true);
//    map2.put(1,1);
//    map2.put(2,2);
//    map2.put(3,3);
//    map2.put(4,4);
//
//    map2.entrySet().forEach(a->System.out.println(a));
//    System.out.println();
//
//    map2.get(2);
//
//    map2.entrySet().forEach(a->System.out.println(a));

//    BubbleLRULinkedHashMap<Range, Integer> map3 = new BubbleLRULinkedHashMap<>(16, 0.75f, true);
    Range range1 = new Range(10, 20);
    Range range2 = new Range(30, 40);
    Range range3 = new Range(5, 45);
//    map3.put(range1, 10);
//    map3.put(range2, 20);
//    System.out.println(map3.containsKey(range3));

    SortedSet<Range> set = new TreeSet<Range>(new Comparator<Range>() {
      @Override
      public int compare(Range o1, Range o2) {
        if (o1.getMin() != o2.getMin()) {
          return o1.getMin() - o2.getMin();
        } else {
          return o1.getMax() - o2.getMax();
        }
      }
    });
    set.add(range1);
    set.add(range2);
    set.add(range3);
    Range range4 = new Range(5, 5);
    Range range5 = new Range(45, 45);
//    System.out.println(set);
    System.out.println(set.subSet(range4, range5));
  }
}
