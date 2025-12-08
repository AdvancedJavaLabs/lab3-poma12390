package se.ifmo.mapreduce.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.ifmo.mapreduce.MapContext;
import se.ifmo.mapreduce.Mapper;
import se.ifmo.model.CategoryStats;
import se.ifmo.model.SalesRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class MapPhaseRunner {

    private static final Logger logger = LoggerFactory.getLogger(MapPhaseRunner.class);

    private final int threadsCount;

    public MapPhaseRunner(int threadsCount) {
        if (threadsCount <= 0) {
            throw new IllegalArgumentException("threadsCount must be > 0");
        }
        this.threadsCount = threadsCount;
    }

    public void run(Collection<SalesRecord> records,
                    Mapper<SalesRecord, String, CategoryStats> mapper,
                    MapContext<String, CategoryStats> context) {

        Objects.requireNonNull(records, "records must not be null");
        Objects.requireNonNull(mapper, "mapper must not be null");
        Objects.requireNonNull(context, "context must not be null");

        if (records.isEmpty()) {
            logger.info("MapPhaseRunner: no records to process, skipping Map phase");
            return;
        }

        List<SalesRecord> recordList = (records instanceof List)
                ? (List<SalesRecord>) records
                : new ArrayList<>(records);

        int total = recordList.size();
        int chunkSize = calculateChunkSize(total, threadsCount);

        logger.info("MapPhaseRunner: starting Map phase for {} records, threadsCount={}, chunkSize={}",
                total, threadsCount, chunkSize);

        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);

        try {
            for (int start = 0; start < total; start += chunkSize) {
                int end = Math.min(start + chunkSize, total);
                List<SalesRecord> chunk = recordList.subList(start, end);

                Runnable task = createMapTask(chunk, mapper, context);
                executor.submit(task);
            }
        } finally {
            executor.shutdown();
            try {
                boolean finished = executor.awaitTermination(5, TimeUnit.MINUTES);
                if (!finished) {
                    logger.warn("MapPhaseRunner: tasks did not finish in time, forcing shutdownNow()");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                logger.warn("MapPhaseRunner: interrupted while awaiting termination, forcing shutdownNow()", e);
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        logger.info("MapPhaseRunner: Map phase completed");
    }

    private Runnable createMapTask(List<SalesRecord> chunk,
                                   Mapper<SalesRecord, String, CategoryStats> mapper,
                                   MapContext<String, CategoryStats> context) {

        return () -> {
            for (SalesRecord record : chunk) {
                if (record == null) {
                    continue;
                }
                try {
                    mapper.map(record, context);
                } catch (RuntimeException e) {
                    logger.error("MapPhaseRunner: error while mapping record {}", record, e);
                }
            }
        };
    }

    private static int calculateChunkSize(int totalRecords, int threadsCount) {
        if (totalRecords <= 0) {
            return 1;
        }
        int size = (int) Math.ceil(totalRecords / (double) threadsCount);
        return Math.max(1, size);
    }
}
