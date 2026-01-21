/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.common;

import java.util.Locale;

/**
 * PromQL function types.
 *
 * <p>Based on the official Prometheus function list:
 * https://prometheus.io/docs/prometheus/latest/querying/functions/
 *
 * <p>Functions are categorized by their purpose:
 * <ul>
 *   <li>Rate functions: rate, irate, increase</li>
 *   <li>Math functions: abs, ceil, floor, round, sqrt, etc.</li>
 *   <li>Time functions: day_of_month, day_of_week, year, etc.</li>
 *   <li>Aggregation over time: avg_over_time, sum_over_time, etc.</li>
 *   <li>Other: absent, changes, clamp, etc.</li>
 * </ul>
 */
public enum FunctionType {
    // Rate/Counter functions
    RATE("rate"),
    IRATE("irate"),
    INCREASE("increase"),

    // Aggregation over time functions
    AVG_OVER_TIME("avg_over_time"),
    MIN_OVER_TIME("min_over_time"),
    MAX_OVER_TIME("max_over_time"),
    SUM_OVER_TIME("sum_over_time"),
    COUNT_OVER_TIME("count_over_time"),
    QUANTILE_OVER_TIME("quantile_over_time"),
    STDDEV_OVER_TIME("stddev_over_time"),
    STDVAR_OVER_TIME("stdvar_over_time"),
    LAST_OVER_TIME("last_over_time"),
    PRESENT_OVER_TIME("present_over_time"),

    // Math functions
    ABS("abs"),
    CEIL("ceil"),
    FLOOR("floor"),
    ROUND("round"),
    SQRT("sqrt"),
    EXP("exp"),
    LN("ln"),
    LOG2("log2"),
    LOG10("log10"),

    // Trigonometric functions
    ACOS("acos"),
    ACOSH("acosh"),
    ASIN("asin"),
    ASINH("asinh"),
    ATAN("atan"),
    ATANH("atanh"),
    COS("cos"),
    COSH("cosh"),
    SIN("sin"),
    SINH("sinh"),
    TAN("tan"),
    TANH("tanh"),
    DEG("deg"),
    RAD("rad"),
    PI("pi"),  // Constant function

    // Time functions
    TIME("time"),
    TIMESTAMP("timestamp"),
    DAY_OF_MONTH("day_of_month"),
    DAY_OF_WEEK("day_of_week"),
    DAY_OF_YEAR("day_of_year"),
    DAYS_IN_MONTH("days_in_month"),
    HOUR("hour"),
    MINUTE("minute"),
    MONTH("month"),
    YEAR("year"),

    // Other functions
    ABSENT("absent"),
    ABSENT_OVER_TIME("absent_over_time"),
    CHANGES("changes"),
    CLAMP("clamp"),
    CLAMP_MAX("clamp_max"),
    CLAMP_MIN("clamp_min"),
    DELTA("delta"),
    DERIV("deriv"),
    HISTOGRAM_QUANTILE("histogram_quantile"),
    HOLT_WINTERS("holt_winters"),
    PREDICT_LINEAR("predict_linear"),
    RESETS("resets"),
    SCALAR("scalar"),
    SGN("sgn"),
    SORT("sort"),
    SORT_DESC("sort_desc"),
    VECTOR("vector");

    private final String name;

    FunctionType(String name) {
        this.name = name;
    }

    /**
     * Gets the function name.
     * @return the function name
     */
    public String getName() {
        return name;
    }

    /**
     * Parse function type from string.
     * @param name the function name
     * @return the corresponding function type
     */
    public static FunctionType fromString(String name) {
        String normalized = name.toLowerCase(Locale.ROOT);
        for (FunctionType type : values()) {
            if (type.name.equals(normalized)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown function: " + name);
    }

    /**
     * Checks if this function requires a range vector as input.
     * @return true if the function requires a range vector
     */
    public boolean requiresRangeVector() {
        return switch (this) {
            case RATE, IRATE, INCREASE, AVG_OVER_TIME, MIN_OVER_TIME, MAX_OVER_TIME, SUM_OVER_TIME, COUNT_OVER_TIME, QUANTILE_OVER_TIME,
                STDDEV_OVER_TIME, STDVAR_OVER_TIME, LAST_OVER_TIME, PRESENT_OVER_TIME, ABSENT_OVER_TIME, CHANGES, DELTA, DERIV,
                HOLT_WINTERS, PREDICT_LINEAR, RESETS -> true;
            default -> false;
        };
    }

    /**
     * Gets the expected number of vector/expression arguments for this function.
     * @return the number of vector arguments (0 = no vectors, -1 = variable)
     */
    public int getVectorArgumentCount() {
        return switch (this) {
            case TIME, PI -> 0;  // No arguments
            case HISTOGRAM_QUANTILE, QUANTILE_OVER_TIME -> 2;  // φ scalar + 1 vector (but φ is in arguments list)
            default -> 1;  // Most functions take 1 vector
        };
    }

    /**
     * Gets the expected number of scalar arguments for this function.
     * @return the number of scalar arguments (-1 = variable)
     */
    public int getScalarArgumentCount() {
        return switch (this) {
            case HISTOGRAM_QUANTILE, QUANTILE_OVER_TIME, CLAMP_MAX, CLAMP_MIN -> 1;
            case CLAMP -> 2;  // min and max
            case ROUND -> -1; // Optional to_nearest
            default -> 0;
        };
    }

    /**
     * Checks if this function is a rate-like function (rate, irate, increase).
     * @return true if this is a rate function
     */
    public boolean isRateFunction() {
        return this == RATE || this == IRATE || this == INCREASE;
    }

    @Override
    public String toString() {
        return name;
    }
}
