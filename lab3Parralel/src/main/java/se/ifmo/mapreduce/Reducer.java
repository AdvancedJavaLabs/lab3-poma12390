package se.ifmo.mapreduce;

@FunctionalInterface
public interface Reducer<K, V, OUT> {

    OUT reduce(K key, Iterable<V> values);
}
