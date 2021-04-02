package com.nus.cool.core.io.cache.utils;

public class BubbleLRULinkedHashMap<K, V> extends LinkedHashMap<K, V> {
  public BubbleLRULinkedHashMap(int initialCapacity,
      float loadFactor,
      boolean accessOrder) {
    super(initialCapacity, loadFactor, accessOrder);
  }


  @Override
  void afterNodeAccess(Node<K, V> e) { // move node to last
    Entry<K, V> c;
    if (accessOrder && tail != e) {
      Entry<K, V> p =
          (Entry<K, V>) e, b = p.before, a = p.after, last = a;
      // p.after = null;
      if (b == null) {
        head = a;
      } else {
        b.after = a;
      }
      if (a != null) {
        a.before = b;
        c = a.after;
        p.after = c;
        if (c != null) {
          c.before = p;
        } else {
          tail = p;
        }
      } else {
        last = b;
      }
      if (last == null) {
        head = p;
        tail = p;
      } else {
        p.before = last;
        last.after = p;
      }
      // tail = p;
      ++modCount;
    }
  }

}
