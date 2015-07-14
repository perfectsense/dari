package com.psddev.dari.db;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

public class CompoundMetric extends Metric {

    private final Operator operator;
    private Metric left;
    private Metric right;
    private Double fixedLeft;
    private Double fixedRight;

    @SuppressWarnings("deprecation")
    public CompoundMetric(Operator operator, Metric left, Metric right) {
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    @SuppressWarnings("deprecation")
    public CompoundMetric(Operator operator, double left, Metric right) {
        this.operator = operator;
        this.fixedLeft = left;
        this.right = right;
    }

    @SuppressWarnings("deprecation")
    public CompoundMetric(Operator operator, Metric left, double right) {
        this.operator = operator;
        this.left = left;
        this.fixedRight = right;
    }

    @SuppressWarnings("deprecation")
    public CompoundMetric(Operator operator, double left, double right) {
        this.operator = operator;
        this.fixedLeft = left;
        this.fixedRight = right;
    }

    protected Double reduce(double left, double right) {
        return operator.reduce(left, right);
    }

    @Override
    public double getByDimensionBetween(String dimension, DateTime start, DateTime end) {
        double leftAmount = left != null ? left.getByDimensionBetween(dimension, start, end) : fixedLeft != null ? fixedLeft : 0d;
        double rightAmount = right != null ? right.getByDimensionBetween(dimension, start, end) : fixedRight != null ? fixedRight : 0d;
        return reduce(leftAmount, rightAmount);
    }

    @Override
    public boolean isEmptyByDimensionBetween(String dimension, DateTime start, DateTime end) {
        return (left != null ? left.isEmptyByDimensionBetween(dimension, start, end) : false)
                && (right != null ? right.isEmptyByDimensionBetween(dimension, start, end) : false);
    }

    @Override
    public Map<String, Double> groupByDimensionBetween(DateTime start, DateTime end) {
        HashMap<String, Double> result = new HashMap<String, Double>();
        Map<String, Double> leftMap = left != null ? left.groupByDimensionBetween(start, end) : new HashMap<String, Double>();
        Map<String, Double> rightMap = right != null ? right.groupByDimensionBetween(start, end) : new HashMap<String, Double>();
        Set<String> combinedKeys = new HashSet<String>(leftMap.keySet());
        combinedKeys.addAll(rightMap.keySet());
        for (String key : combinedKeys) {
            double leftAmount = left != null ? (leftMap.containsKey(key) ? leftMap.get(key) : 0d) : fixedLeft;
            double rightAmount = right != null ? (rightMap.containsKey(key) ? rightMap.get(key) : 0d) : fixedRight;
            result.put(key, reduce(leftAmount, rightAmount));
        }
        return result;
    }

    @Override
    public Map<DateTime, Double> groupByDate(String dimension, MetricInterval interval, DateTime start, DateTime end) {
        Map<DateTime, Double> result = new HashMap<DateTime, Double>();
        Map<DateTime, Double> leftMap = left != null ? left.groupByDate(dimension, interval, start, end) : new HashMap<DateTime, Double>();
        Map<DateTime, Double> rightMap = right != null ? right.groupByDate(dimension, interval, start, end) : new HashMap<DateTime, Double>();
        Set<DateTime> combinedKeys = new HashSet<DateTime>(leftMap.keySet());
        combinedKeys.addAll(rightMap.keySet());
        for (DateTime key : combinedKeys) {
            double leftAmount = left != null ? leftMap.containsKey(key) ? leftMap.get(key) : 0d : fixedLeft;
            double rightAmount = right != null ? rightMap.containsKey(key) ? rightMap.get(key) : 0d : fixedRight;
            result.put(key, reduce(leftAmount, rightAmount));
        }
        return result;
    }

    public static enum Operator {
        ADD("+") {
            public double reduce(double left, double right) {
                return left + right;
            }
        },

        SUBTRACT("-") {
            public double reduce(double left, double right) {
                return left - right;
            }
        },

        DIVIDE("-") {
            public double reduce(double left, double right) {
                return right == 0d ? 0d : left / right;
            }
        },

        MULTIPLY("*") {
            public double reduce(double left, double right) {
                return left * right;
            }
        };

        private final String symbol;

        private Operator(String symbol) {
            this.symbol = symbol;
        }

        public String getSymbol() {
            return symbol;
        }

        public abstract double reduce(double left, double right);
    }

    // -- invalid operations for CompoundMetrics
    @Override
    @Deprecated
    public State getOwner() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public ObjectField getField() {
        throw new UnsupportedOperationException();
    }
}
