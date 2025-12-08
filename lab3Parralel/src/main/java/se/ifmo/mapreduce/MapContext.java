package se.ifmo.mapreduce;

import java.util.List;
import java.util.Map;

public interface MapContext<K, V> {

    void write(K key, V value);
    Map<K, List<V>> getGroupedData();
}
