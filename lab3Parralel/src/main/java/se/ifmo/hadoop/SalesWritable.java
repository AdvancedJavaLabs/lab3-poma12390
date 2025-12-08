package se.ifmo.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class SalesWritable implements Writable {

    private double totalRevenue;
    private long totalQuantity;

    public SalesWritable() {
    }

    public SalesWritable(double totalRevenue, long totalQuantity) {
        this.totalRevenue = totalRevenue;
        this.totalQuantity = totalQuantity;
    }

    public double getTotalRevenue() {
        return totalRevenue;
    }

    public long getTotalQuantity() {
        return totalQuantity;
    }

    public void set(double totalRevenue, long totalQuantity) {
        this.totalRevenue = totalRevenue;
        this.totalQuantity = totalQuantity;
    }

    public void addInPlace(SalesWritable other) {
        if (other == null) {
            return;
        }
        this.totalRevenue += other.totalRevenue;
        this.totalQuantity += other.totalQuantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(totalRevenue);
        out.writeLong(totalQuantity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.totalRevenue = in.readDouble();
        this.totalQuantity = in.readLong();
    }

    @Override
    public String toString() {
        return totalRevenue + "\t" + totalQuantity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SalesWritable)) return false;
        SalesWritable that = (SalesWritable) o;
        return Double.compare(that.totalRevenue, totalRevenue) == 0 &&
                totalQuantity == that.totalQuantity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalRevenue, totalQuantity);
    }
}
