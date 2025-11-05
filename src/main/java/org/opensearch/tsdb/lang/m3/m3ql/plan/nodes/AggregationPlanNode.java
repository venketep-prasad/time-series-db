/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.AggregationType;
import org.opensearch.tsdb.lang.m3.common.Utils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Represents an aggregation plan node in the M3QL query plan. Aggregations generally follow the pattern:
 * <pre>
 *   aggFunction tag1 tag2
 * </pre>
 */
public class AggregationPlanNode extends M3PlanNode {
    private final AggregationType aggType;
    private final List<String> tags;

    /**
     * Constructs an AggPlanNode with the specified aggregation type and tags.
     *
     * @param id node id
     * @param aggType The type of aggregation (e.g., SUM, AVG)
     * @param tags The list of tags to be aggregate by
     */
    public AggregationPlanNode(int id, AggregationType aggType, List<String> tags) {
        super(id);
        this.aggType = aggType;
        this.tags = tags;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "AGG(%s, groupBy=%s)", aggType, tags);
    }

    /**
     * Returns a list of aggregation keys
     * @return List of Strings representing the aggregation key tags, null indicates aggregate all series without grouping.
     */
    public List<String> getTags() {
        return tags;
    }

    /**
     * Returns the aggregation type of this node.
     * @return AggregationType representing the type of aggregation (e.g., SUM, AVG).
     */
    public AggregationType getAggregationType() {
        return aggType;
    }

    /**
     * Creates the aggregation plan node from the corresponding AST Node
     * @param functionNode The function AST node representing the aggregation
     * @param type The type of aggregation
     * @return The constructed AggregationPlanNode
     */
    public static AggregationPlanNode of(FunctionNode functionNode, AggregationType type) {
        if (functionNode.getChildren() == null) {
            return new AggregationPlanNode(M3PlannerContext.generateId(), type, null);
        }

        List<String> groupByExpressions = new ArrayList<>();
        for (M3ASTNode astNode : functionNode.getChildren()) {
            if (astNode instanceof ValueNode node) {
                groupByExpressions.add(Utils.stripDoubleQuotes(node.getValue()));
            }
        }
        return new AggregationPlanNode(M3PlannerContext.generateId(), type, groupByExpressions);
    }
}
