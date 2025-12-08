package se.ifmo.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.ifmo.model.SalesRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public final class SalesCsvReader {

    private static final Logger logger = LoggerFactory.getLogger(SalesCsvReader.class);

    private SalesCsvReader() {
    }

    public static List<SalesRecord> readAllFromDirectory(String directoryPath) throws IOException {
        return readAllFromDirectory(Paths.get(directoryPath));
    }

    public static List<SalesRecord> readAllFromDirectory(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            throw new IOException("Directory does not exist: " + directory);
        }
        if (!Files.isDirectory(directory)) {
            throw new IOException("Path is not a directory: " + directory);
        }

        List<SalesRecord> result = new ArrayList<>();

        try (Stream<Path> files = Files.list(directory)) {
            files
                    .filter(path -> Files.isRegularFile(path))
                    .filter(path -> path.getFileName().toString().toLowerCase().endsWith(".csv"))
                    .forEach(path -> {
                        try {
                            logger.info("Reading CSV file: {}", path);
                            readFromFile(path, result);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }

        return result;
    }

    public static List<SalesRecord> readAllFromResourceDirectory(int filesCount) throws IOException {
        if (filesCount < 0) {
            throw new IllegalArgumentException("filesCount must be non-negative");
        }

        List<SalesRecord> result = new ArrayList<>();
        for (int i = 0; i < filesCount; i++) {
            String resourcePath = i + ".csv";
            logger.info("Reading CSV resource: {}", resourcePath);
            readFromResource(resourcePath, result);
        }
        return result;
    }

    private static void readFromFile(Path file, List<SalesRecord> target) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            readFromReader(reader, target);
        }
    }

    private static void readFromResource(String resourcePath, List<SalesRecord> target) throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = SalesCsvReader.class.getClassLoader();
        }

        try (InputStream is = classLoader.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IOException("Resource not found: " + resourcePath);
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                readFromReader(reader, target);
            }
        }
    }

    private static void readFromReader(BufferedReader reader, List<SalesRecord> target) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }

            try {
                SalesRecord record = SalesRecord.fromCsvLine(trimmed);
                if (record != null) {
                    target.add(record);
                }
            } catch (IllegalArgumentException e) {
                logger.warn("Failed to parse CSV line: '{}'. Reason: {}", trimmed, e.getMessage());
            }
        }
    }
}
