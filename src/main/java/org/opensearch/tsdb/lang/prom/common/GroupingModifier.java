/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.common;

/**
 * Aggregation grouping modifiers in PromQL.
 */
public enum GroupingModifier {
    /**
     * Keep only the specified labels in the result.
     */
    BY,

    /**
     * Remove the specified labels, keep all others.
     */
    WITHOUT
}
