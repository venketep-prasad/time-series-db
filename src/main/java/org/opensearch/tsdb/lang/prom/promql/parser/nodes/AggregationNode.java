/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.promql.parser.nodes;

import org.opensearch.tsdb.lang.prom.common.GroupingModifier;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents an aggregation operation in PromQL.
 * Examples: sum by (job, instance) (http_requests_total), sum(http_requests_total) by (job)
 */
public class AggregationNode extends PromASTNode {
    /**
     * The aggregation function name (sum, avg, min, max, count).
     */
    private final String aggregationType;

    /**
     * The grouping modifier (BY, WITHOUT).
     */
    private final GroupingModifier groupingModifier;

    /**
     * The list of labels to group by (empty if no grouping).
     */
    private final List<String> groupingLabels;

    /**
     * Constructor for AggregationNode.
     * @param aggregationType the aggregation function name (e.g., "sum", "avg")
     * @param groupingModifier the grouping modifier (BY, WITHOUT, or null for no grouping)
     * @param groupingLabels the list of label names for grouping (can be null or empty)
     */
    public AggregationNode(String aggregationType, GroupingModifier groupingModifier, List<String> groupingLabels) {
        super();
        this.aggregationType = aggregationType;
        this.groupingModifier = groupingModifier;
        this.groupingLabels = groupingLabels != null ? groupingLabels : new ArrayList<>();
    }

    /**
     * Gets the aggregation type.
     * @return the aggregation function name
     */
    public String getAggregationType() {
        return aggregationType;
    }

    /**
     * Gets the grouping modifier.
     * @return the grouping modifier (BY, WITHOUT), or null if no grouping
     */
    public GroupingModifier getGroupingModifier() {
        return groupingModifier;
    }

    /**
     * Gets the grouping labels.
     * @return the list of label names for grouping
     */
    public List<String> getGroupingLabels() {
        return groupingLabels;
    }

    /**
     * Sets the expression to be aggregated.
     * @param expression the expression node to aggregate
     */
    public void setExpression(PromASTNode expression) {
        addChild(expression);
    }

    /**
     * Gets the expression being aggregated.
     * @return the expression node, or null if not set
     */
    public PromASTNode getExpression() {
        return children.isEmpty() ? null : children.getFirst();
    }

    @Override
    public <T> T accept(PromASTVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
