package se.ifmo.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.ifmo.model.SalesRecord;

import java.io.IOException;
import java.math.BigDecimal;

public class HadoopSalesMapper extends Mapper<LongWritable, Text, Text, SalesWritable> {

    private static final Logger logger = LoggerFactory.getLogger(HadoopSalesMapper.class);

    private final Text outKey = new Text();
    private final SalesWritable outValue = new SalesWritable();

    @Override
    protected void map(LongWritable key,
                       Text value,
                       Context context) throws IOException, InterruptedException {

        String line = value.toString();
        if (line == null) {
            return;
        }

        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            return;
        }

        SalesRecord record;
        try {
            record = SalesRecord.fromCsvLine(trimmed);
        } catch (IllegalArgumentException e) {
            logger.warn("Failed to parse CSV line '{}': {}", trimmed, e.getMessage());
            return;
        }

        if (record == null) {
            return;
        }

        BigDecimal revenue = record.price()
                .multiply(BigDecimal.valueOf(record.quantity()));
        double revenueDouble = revenue.doubleValue();
        long quantity = record.quantity();

        outKey.set(record.category());
        outValue.set(revenueDouble, quantity);

        context.write(outKey, outValue);
    }
}
