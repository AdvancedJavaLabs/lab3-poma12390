package se.ifmo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.ifmo.io.SalesCsvReader;
import se.ifmo.model.CategoryStats;
import se.ifmo.model.SalesRecord;
import se.ifmo.mapreduce.InMemoryMapContext;
import se.ifmo.mapreduce.MapContext;
import se.ifmo.mapreduce.Mapper;
import se.ifmo.mapreduce.Reducer;
import se.ifmo.mapreduce.runtime.MapPhaseRunner;
import se.ifmo.mapreduce.runtime.ReducePhaseRunner;
import se.ifmo.mapreduce.sales.SalesMapper;
import se.ifmo.mapreduce.sales.SalesReducer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class SalesAnalysisApp {

    private static final Logger LOG = LoggerFactory.getLogger(SalesAnalysisApp.class);

    private static final String INPUT_DIRECTORY = "src/main/resources";

    private static final String OUTPUT_RESULT_FILE = "output/result.txt";

    private static final String OUTPUT_PERFORMANCE_FILE = "output/performance.csv";

    private static final int DEFAULT_MAP_THREADS = 4;
    private static final int DEFAULT_REDUCE_THREADS = 4;

    public static void main(String[] args) {
        SalesAnalysisApp app = new SalesAnalysisApp();
        try {
            List<SalesRecord> records = SalesCsvReader.readAllFromDirectory(INPUT_DIRECTORY);
            LOG.info("Loaded {} sales records from '{}'", records.size(), INPUT_DIRECTORY);

            app.runSingleAnalysis(records, DEFAULT_MAP_THREADS, DEFAULT_REDUCE_THREADS);

            int[] mapThreadProfiles = {1, 2, 4, 8};
            int[] reduceThreadProfiles = {1, 2, 4, 8};
            app.runPerformanceExperiments(records, mapThreadProfiles, reduceThreadProfiles);

        } catch (Exception e) {
            LOG.error("Failed to run SalesAnalysisApp", e);
            System.exit(1);
        }
    }

    private void runSingleAnalysis(List<SalesRecord> records,
                                   int mapThreads,
                                   int reduceThreads) throws IOException, InterruptedException {

        LOG.info("Running single analysis with mapThreads={}, reduceThreads={}",
                mapThreads, reduceThreads);

        long start = System.currentTimeMillis();

        List<Map.Entry<String, CategoryStats>> results =
                executeMapReduce(records, mapThreads, reduceThreads);

        long duration = System.currentTimeMillis() - start;

        LOG.info("Single analysis finished in {} ms, categories={}", duration, results.size());

        writeResultsToFile(results, OUTPUT_RESULT_FILE);
    }

    private List<Map.Entry<String, CategoryStats>> executeMapReduce(List<SalesRecord> records,
                                                                    int mapThreads,
                                                                    int reduceThreads) throws InterruptedException {

        Mapper<SalesRecord, String, CategoryStats> mapper = new SalesMapper();
        Reducer<String, CategoryStats, CategoryStats> reducer = new SalesReducer();

        MapContext<String, CategoryStats> mapContext = new InMemoryMapContext<>();

        MapPhaseRunner mapRunner = new MapPhaseRunner(mapThreads);
        ReducePhaseRunner reduceRunner = new ReducePhaseRunner(reduceThreads);

        // Map
        mapRunner.run(records, mapper, mapContext);

        // Reduce
        Map<String, CategoryStats> reduced =
                reduceRunner.runReduce(mapContext.getGroupedData(), reducer);

        return sortByRevenueDesc(reduced);
    }

    private void runPerformanceExperiments(List<SalesRecord> records,
                                           int[] mapThreadProfiles,
                                           int[] reduceThreadProfiles) throws IOException, InterruptedException {

        LOG.info("Running performance experiments for mapThreads={} and reduceThreads={}",
                Arrays.toString(mapThreadProfiles),
                Arrays.toString(reduceThreadProfiles));

        Path perfPath = Paths.get(OUTPUT_PERFORMANCE_FILE);
        Path perfDir = perfPath.getParent();
        if (perfDir != null) {
            Files.createDirectories(perfDir);
        }

        try (BufferedWriter writer = Files.newBufferedWriter(perfPath, StandardCharsets.UTF_8)) {
            writer.write("mapThreads,reduceThreads,durationMillis");
            writer.newLine();

            for (int mapThreads : mapThreadProfiles) {
                for (int reduceThreads : reduceThreadProfiles) {

                    long start = System.currentTimeMillis();

                    List<Map.Entry<String, CategoryStats>> results =
                            executeMapReduce(records, mapThreads, reduceThreads);

                    long duration = System.currentTimeMillis() - start;


                    writer.write(mapThreads + "," + reduceThreads + "," + duration);
                    writer.newLine();
                }
            }
        }

        LOG.info("Performance results written to '{}'", perfPath.toAbsolutePath());
    }

    private List<Map.Entry<String, CategoryStats>> sortByRevenueDesc(
            Map<String, CategoryStats> reducedMap) {

        List<Map.Entry<String, CategoryStats>> list = new ArrayList<>(reducedMap.entrySet());
        list.sort((e1, e2) -> e2.getValue().totalRevenue()
                .compareTo(e1.getValue().totalRevenue()));
        return list;
    }

    private void writeResultsToFile(List<Map.Entry<String, CategoryStats>> results,
                                    String outputFilePath) throws IOException {

        Path outPath = Paths.get(outputFilePath);
        Path parent = outPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        try (BufferedWriter writer = Files.newBufferedWriter(outPath, StandardCharsets.UTF_8)) {
            writer.write(String.format("%-15s %-12s %-10s%n", "Category", "Revenue", "Quantity"));

            for (Map.Entry<String, CategoryStats> entry : results) {
                String category = entry.getKey();
                CategoryStats stats = entry.getValue();

                BigDecimal revenue = stats.totalRevenue();
                long quantity = stats.totalQuantity();

                String revenueStr = String.format(Locale.US, "%.2f", revenue);

                writer.write(String.format(
                        "%-15s %-12s %-10d%n",
                        category,
                        revenueStr,
                        quantity
                ));
            }
        }
    }
}