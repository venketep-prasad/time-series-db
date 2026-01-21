/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.promql.plan.nodes;

import org.opensearch.tsdb.lang.prom.common.AggregationType;
import org.opensearch.tsdb.lang.prom.common.GroupingModifier;

import java.util.ArrayList;
import java.util.List;

/**
 * Plan node representing an aggregation operation.
 */
public class AggregationPlanNode extends PromPlanNode {
    private final AggregationType aggregationType;
    private final GroupingModifier groupingModifier;  // null if no grouping
    private final List<String> groupingLabels;

    public AggregationPlanNode(int id, AggregationType aggregationType, GroupingModifier groupingModifier, List<String> groupingLabels) {
        super(id);
        this.aggregationType = aggregationType;
        this.groupingModifier = groupingModifier;
        this.groupingLabels = groupingLabels != null ? groupingLabels : new ArrayList<>();
    }

    public AggregationType getAggregationType() {
        return aggregationType;
    }

    public GroupingModifier getGroupingModifier() {
        return groupingModifier;
    }

    public List<String> getGroupingLabels() {
        return groupingLabels;
    }

    @Override
    public <T> T accept(PromPlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "Aggregation[" + aggregationType.toString() + "]";
    }
}
