/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

/**
 * Enum representing different value types for time series data.
 * Used to categorize the type of data stored in samples.
 */
public enum ValueType {
    /** 64-bit floating-point values (double precision) */
    FLOAT64,
    /** Histogram data */
    // TODO: HISTOGRAM,
    /** 64-bit floating-point histogram data */
    // TODO: FLOAT64_HISTOGRAM,
}
