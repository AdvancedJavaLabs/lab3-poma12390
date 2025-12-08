package se.ifmo.mapreduce.sales;

import se.ifmo.mapreduce.MapContext;
import se.ifmo.mapreduce.Mapper;
import se.ifmo.model.CategoryStats;
import se.ifmo.model.SalesRecord;

import java.math.BigDecimal;


public class SalesMapper implements Mapper<SalesRecord, String, CategoryStats> {

    @Override
    public void map(SalesRecord input, MapContext<String, CategoryStats> context) {
        if (input == null) {
            return;
        }

        String category = input.category();
        BigDecimal price = input.price();
        int quantity = input.quantity();

        BigDecimal revenue = price.multiply(BigDecimal.valueOf(quantity));

        CategoryStats stats = CategoryStats.of(revenue, quantity);

        context.write(category, stats);
    }
}
