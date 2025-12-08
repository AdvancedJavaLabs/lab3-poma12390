package se.ifmo.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public final class HadoopSalesJob {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopSalesJob.class);

    private static final String SAMPLE_RESOURCE_NAME = "0.csv";

    private static final String OUTPUT_DIR = "output/hadoop-result";

    private HadoopSalesJob() {
    }

    public static void main(String[] args) throws Exception {
        String resourcesRoot = resolveResourcesRoot();
        LOG.info("Resolved resources root from '{}': '{}'", SAMPLE_RESOURCE_NAME, resourcesRoot);

        String inputPath = resourcesRoot;
        String outputPath = OUTPUT_DIR;

        LOG.info(
                "Starting HadoopSalesJob with resources root resolved from '{}': inputPath='{}', output='{}'",
                SAMPLE_RESOURCE_NAME,
                inputPath,
                outputPath
        );

        int exitCode = run(inputPath, outputPath);
        System.exit(exitCode);
    }

    private static String resolveResourcesRoot() {
        URL resourceUrl = Thread.currentThread()
                .getContextClassLoader()
                .getResource(SAMPLE_RESOURCE_NAME);

        if (resourceUrl == null) {
            throw new IllegalStateException("Resource '" + SAMPLE_RESOURCE_NAME + "' not found on classpath");
        }

        try {
            java.nio.file.Path path = Paths.get(resourceUrl.toURI()).getParent();

            return path.toFile().getAbsolutePath();
        } catch (URISyntaxException e) {
            throw new IllegalStateException(
                    "Failed to resolve resources root from '" + SAMPLE_RESOURCE_NAME + "'",
                    e
            );
        }
    }

    public static int run(String inputDir, String outputDir) throws Exception {
        Configuration conf = new Configuration();

        // Локальный режим
        conf.set("mapreduce.framework.name", "local");

        conf.set("fs.defaultFS", "file:///");

        Job job = Job.getInstance(conf, "SalesAnalysis-Hadoop");
        job.setJarByClass(HadoopSalesJob.class);

        job.setMapperClass(HadoopSalesMapper.class);
        job.setReducerClass(HadoopSalesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SalesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        File input = new File(inputDir);
        if (!input.exists() || !input.isDirectory()) {
            throw new IllegalArgumentException("Input directory does not exist or is not a directory: " + inputDir);
        }
        FileInputFormat.addInputPath(job, new Path(input.getAbsolutePath()));

        File outDir = new File(outputDir);
        if (outDir.exists()) {
            deleteRecursively(outDir);
        }
        FileOutputFormat.setOutputPath(job, new Path(outDir.getAbsolutePath()));

        LOG.info("Submitting Hadoop job: input={}, output={}", input.getAbsolutePath(), outDir.getAbsolutePath());

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    private static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File c : children) {
                    deleteRecursively(c);
                }
            }
        }
        if (!file.delete() && file.exists()) {
            LOG.warn("Failed to delete {}", file);
        }
    }
}
