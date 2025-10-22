/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

/**
 * The different types of window aggregations supported in M3QL.
 * <p>
 * Used in window aggregation functions like moving, summarize, etc.
 */
public enum WindowAggregationType {
    SUM,
    AVG,
    MIN,
    MAX,
    MEDIAN;

    public static WindowAggregationType fromString(String aggType) {
        return switch (aggType) {
            case "avg", "average" -> AVG;
            case "max", "maximum" -> MAX;
            case "median" -> MEDIAN;
            case "min", "minimum" -> MIN;
            case "sum" -> SUM;
            default -> throw new IllegalArgumentException("Invalid window aggregation type: " + aggType);
        };
    }
}
