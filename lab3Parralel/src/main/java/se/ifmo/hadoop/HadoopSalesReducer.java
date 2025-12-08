package se.ifmo.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class HadoopSalesReducer extends Reducer<Text, SalesWritable, Text, Text> {

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key,
                          Iterable<SalesWritable> values,
                          Context context) throws IOException, InterruptedException {

        double totalRevenue = 0.0;
        long totalQuantity = 0L;

        for (SalesWritable v : values) {
            if (v == null) {
                continue;
            }
            totalRevenue += v.getTotalRevenue();
            totalQuantity += v.getTotalQuantity();
        }

        BigDecimal revenueBD = BigDecimal.valueOf(totalRevenue)
                .setScale(2, RoundingMode.HALF_UP);

        String result = revenueBD.toPlainString() + "\t" + totalQuantity;
        outValue.set(result);

        context.write(key, outValue);
    }
}
