package com.nus.cool.core.io.cache;

import com.nus.cool.core.io.cache.utils.BubbleLRULinkedHashMap;
import java.util.LinkedHashMap;

public class BubbleLRUTest {

  public static void main(String[] args) {
    LinkedHashMap<Integer, Integer> map = new LinkedHashMap<>(16, 0.75f, true);
    map.put(1,1);
    map.put(2,2);
    map.put(3,3);
    map.put(4,4);

    map.entrySet().forEach(a->System.out.println(a));
    System.out.println();

    map.get(2);

    map.entrySet().forEach(a->System.out.println(a));
    System.out.println();

    BubbleLRULinkedHashMap<Integer, Integer> map2 = new BubbleLRULinkedHashMap<>(16, 0.75f, true);
    map2.put(1,1);
    map2.put(2,2);
    map2.put(3,3);
    map2.put(4,4);

    map2.entrySet().forEach(a->System.out.println(a));
    System.out.println();

    map2.get(2);

    map2.entrySet().forEach(a->System.out.println(a));
  }
}
