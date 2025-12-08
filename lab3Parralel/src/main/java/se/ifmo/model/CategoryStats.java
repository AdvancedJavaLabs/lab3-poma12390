package se.ifmo.model;

import java.math.BigDecimal;
import java.util.Objects;

public record CategoryStats(BigDecimal totalRevenue, long totalQuantity) {

    public CategoryStats {
        if (totalRevenue == null) {
            throw new IllegalArgumentException("totalRevenue must not be null");
        }
    }

    public static CategoryStats of(BigDecimal totalRevenue, long totalQuantity) {
        return new CategoryStats(totalRevenue, totalQuantity);
    }

    public static CategoryStats empty() {
        return new CategoryStats(BigDecimal.ZERO, 0L);
    }

    public CategoryStats add(CategoryStats other) {
        if (other == null) {
            return this;
        }
        BigDecimal newRevenue = this.totalRevenue.add(other.totalRevenue);
        long newQuantity = this.totalQuantity + other.totalQuantity;
        return new CategoryStats(newRevenue, newQuantity);
    }

    @Override
    public String toString() {
        return "CategoryStats{" +
                "totalRevenue=" + totalRevenue +
                ", totalQuantity=" + totalQuantity +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CategoryStats)) return false;
        CategoryStats that = (CategoryStats) o;
        return totalQuantity == that.totalQuantity &&
                totalRevenue.compareTo(that.totalRevenue) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalRevenue.stripTrailingZeros(), totalQuantity);
    }
}
