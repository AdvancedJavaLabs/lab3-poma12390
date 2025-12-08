package se.ifmo.mapreduce;


@FunctionalInterface
public interface Mapper<IN, K, V> {

    void map(IN input, MapContext<K, V> context);
}
