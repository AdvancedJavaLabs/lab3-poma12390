package se.ifmo.mapreduce.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.ifmo.mapreduce.Reducer;
import se.ifmo.model.CategoryStats;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ReducePhaseRunner {

    private static final Logger logger = LoggerFactory.getLogger(ReducePhaseRunner.class);

    private final int threadsCount;

    public ReducePhaseRunner(int threadsCount) {
        if (threadsCount <= 0) {
            throw new IllegalArgumentException("threadsCount must be > 0");
        }
        this.threadsCount = threadsCount;
    }

    public Map<String, CategoryStats> runReduce(Map<String, List<CategoryStats>> groupedData,
                                                Reducer<String, CategoryStats, CategoryStats> reducer) {

        Objects.requireNonNull(groupedData, "groupedData must not be null");
        Objects.requireNonNull(reducer, "reducer must not be null");

        if (groupedData.isEmpty()) {
            logger.info("ReducePhaseRunner: no keys to reduce, returning empty result");
            return Map.of();
        }

        List<String> keys = new ArrayList<>(groupedData.keySet());
        int totalKeys = keys.size();
        int chunkSize = calculateChunkSize(totalKeys, threadsCount);

        logger.info("ReducePhaseRunner: starting Reduce phase for {} keys, threadsCount={}, chunkSize={}",
                totalKeys, threadsCount, chunkSize);

        ConcurrentHashMap<String, CategoryStats> result = new ConcurrentHashMap<>();

        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);

        try {
            for (int start = 0; start < totalKeys; start += chunkSize) {
                int end = Math.min(start + chunkSize, totalKeys);
                List<String> chunkKeys = keys.subList(start, end);

                Runnable task = createReduceTask(chunkKeys, groupedData, reducer, result);
                executor.submit(task);
            }
        } finally {
            executor.shutdown();
            try {
                boolean finished = executor.awaitTermination(5, TimeUnit.MINUTES);
                if (!finished) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        logger.info("ReducePhaseRunner: Reduce phase completed, result size={}", result.size());
        return result;
    }


    private Runnable createReduceTask(List<String> chunkKeys,
                                      Map<String, List<CategoryStats>> groupedData,
                                      Reducer<String, CategoryStats, CategoryStats> reducer,
                                      ConcurrentHashMap<String, CategoryStats> result) {

        return () -> {
            for (String key : chunkKeys) {
                if (key == null) {
                    continue;
                }
                List<CategoryStats> values = groupedData.get(key);
                if (values == null || values.isEmpty()) {
                    continue;
                }

                try {
                    CategoryStats reduced = reducer.reduce(key, values);
                    if (reduced != null) {
                        result.put(key, reduced);
                    }
                } catch (RuntimeException e) {
                    logger.error("ReducePhaseRunner: error while reducing key '{}'", key, e);
                }
            }
        };
    }

    private static int calculateChunkSize(int totalKeys, int threadsCount) {
        if (totalKeys <= 0) {
            return 1;
        }
        int size = (int) Math.ceil(totalKeys / (double) threadsCount);
        return Math.max(1, size);
    }
}
