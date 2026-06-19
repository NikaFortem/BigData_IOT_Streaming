package com.hse.bigdata;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MedianHumidityAgg extends AggregateFunction<Double, MedianHumidityAgg.Accumulator> {

    public static class Accumulator {
        public List<Double> values = new ArrayList<>();
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    public void accumulate(Accumulator acc, Double value) {
        if (value != null) {
            acc.values.add(value);
        }
    }

    @Override
    public Double getValue(Accumulator acc) {
        if (acc.values == null || acc.values.isEmpty()) {
            return null;
        }

        List<Double> sorted = new ArrayList<>(acc.values);
        Collections.sort(sorted);

        int n = sorted.size();
        int mid = n / 2;

        if (n % 2 == 1) {
            return sorted.get(mid);
        }

        return (sorted.get(mid - 1) + sorted.get(mid)) / 2.0;
    }
}
