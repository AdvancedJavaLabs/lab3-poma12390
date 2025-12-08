package se.ifmo.mapreduce.sales;

import se.ifmo.mapreduce.Reducer;
import se.ifmo.model.CategoryStats;

public class SalesReducer implements Reducer<String, CategoryStats, CategoryStats> {

    @Override
    public CategoryStats reduce(String key, Iterable<CategoryStats> values) {
        CategoryStats total = CategoryStats.empty();
        if (values == null) {
            return total;
        }

        for (CategoryStats stats : values) {
            if (stats != null) {
                total = total.add(stats);
            }
        }

        return total;
    }
}
