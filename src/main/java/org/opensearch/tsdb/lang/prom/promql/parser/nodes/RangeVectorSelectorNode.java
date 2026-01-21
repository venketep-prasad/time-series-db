/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.promql.parser.nodes;

/**
 * Represents a range vector selector in PromQL.
 * Example: http_requests_total[5m]
 */
public class RangeVectorSelectorNode extends VectorSelectorNode {
    /**
     * The time range duration in milliseconds.
     */
    private final long rangeMs;

    /**
     * Constructor for RangeVectorSelectorNode.
     * @param metricName the name of the metric to select
     * @param rangeMs the time range duration in milliseconds
     */
    public RangeVectorSelectorNode(String metricName, long rangeMs) {
        super(metricName);
        this.rangeMs = rangeMs;
    }

    /**
     * Gets the time range duration in milliseconds.
     * @return the range duration in milliseconds
     */
    public long getRangeMs() {
        return rangeMs;
    }

    @Override
    public <T> T accept(PromASTVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
