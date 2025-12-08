package se.ifmo.model;

import java.math.BigDecimal;
import java.util.Locale;

public record SalesRecord(long transactionId, long productId, String category, BigDecimal price, int quantity) {

    public SalesRecord {
        if (category == null || category.isBlank()) {
            throw new IllegalArgumentException("Category must not be null or blank");
        }
        if (price == null) {
            throw new IllegalArgumentException("Price must not be null");
        }
        if (quantity < 0) {
            throw new IllegalArgumentException("Quantity must be non-negative");
        }

    }
    public static SalesRecord fromCsvLine(String line) {
        if (line == null) {
            throw new IllegalArgumentException("CSV line must not be null");
        }

        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("CSV line must not be empty");
        }

        String lower = trimmed.toLowerCase(Locale.ROOT);
        if (lower.startsWith("transaction_id")) {
            return null;
        }

        // transaction_id,product_id,category,price,quantity
        String[] parts = trimmed.split(",", -1);
        if (parts.length < 5) {
            throw new IllegalArgumentException(
                    "Expected 5 columns, but got " + parts.length + " for line: '" + line + "'"
            );
        }

        try {
            long transactionId = Long.parseLong(parts[0].trim());
            long productId = Long.parseLong(parts[1].trim());
            String category = parts[2].trim();
            String priceStr = parts[3].trim();
            String quantityStr = parts[4].trim();

            if (category.isEmpty()) {
                throw new IllegalArgumentException("Category column is empty for line: '" + line + "'");
            }

            BigDecimal price = new BigDecimal(priceStr);
            int quantity = Integer.parseInt(quantityStr);

            return new SalesRecord(transactionId, productId, category, price, quantity);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Failed to parse numeric value in line: '" + line + "'", e
            );
        }
    }

    @Override
    public String toString() {
        return "SalesRecord{" +
                "transactionId=" + transactionId +
                ", productId=" + productId +
                ", category='" + category + '\'' +
                ", price=" + price +
                ", quantity=" + quantity +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SalesRecord)) return false;
        SalesRecord that = (SalesRecord) o;
        return transactionId == that.transactionId &&
                productId == that.productId &&
                quantity == that.quantity &&
                category.equals(that.category) &&
                price.equals(that.price);
    }

}
