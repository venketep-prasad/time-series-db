/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.promql.parser.nodes;

/**
 * Represents an instant vector selector in PromQL.
 * Example: http_requests_total{job="api", method="GET"}
 */
public class InstantVectorSelectorNode extends VectorSelectorNode {

    /**
     * Constructor for InstantVectorSelectorNode.
     * @param metricName the name of the metric to select
     */
    public InstantVectorSelectorNode(String metricName) {
        super(metricName);
    }

    @Override
    public <T> T accept(PromASTVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
