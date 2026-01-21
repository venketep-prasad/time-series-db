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
 * Supported PromQL aggregation types.
 */
public enum AggregationType {
    SUM,
    AVG,
    MIN,
    MAX,
    COUNT;

    /**
     * Parse aggregation type from string.
     */
    public static AggregationType fromString(String type) {
        try {
            return valueOf(type.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown aggregation type: " + type);
        }
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
