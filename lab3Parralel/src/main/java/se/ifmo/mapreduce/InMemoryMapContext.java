package se.ifmo.mapreduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryMapContext<K, V> implements MapContext<K, V> {

    private final ConcurrentHashMap<K, List<V>> storage = new ConcurrentHashMap<>();

    @Override
    public void write(K key, V value) {
        List<V> list = storage.computeIfAbsent(
                key,
                k -> Collections.synchronizedList(new ArrayList<>())
        );
        list.add(value);
    }

    @Override
    public Map<K, List<V>> getGroupedData() {
        return storage;
    }
}
