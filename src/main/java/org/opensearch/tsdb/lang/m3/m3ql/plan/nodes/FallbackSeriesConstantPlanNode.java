/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.List;
import java.util.Locale;

/**
 * FallbackSeriesConstantPlanNode represents a node in the M3QL plan that returns its input
 * series if it is not empty, otherwise returns a constant time series filled with the specified value.
 *
 * <p>This is the unary variant of fallbackSeries that takes a numeric constant as replacement.
 * Example: {@code fetch a | fallbackSeries 1.0}
 *
 * <p>This node is useful in auto-generated alerts where the input series may not always exist.
 */
public class FallbackSeriesConstantPlanNode extends M3PlanNode {

    private final double constantValue;

    /**
     * Constructor for FallbackSeriesConstantPlanNode.
     *
     * @param id node id
     * @param constantValue the constant value to fill the fallback series with
     */
    public FallbackSeriesConstantPlanNode(int id, double constantValue) {
        super(id);
        this.constantValue = constantValue;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.getDefault(), "FALLBACK_SERIES(%s)", constantValue);
    }

    /**
     * Returns the constant value for the fallback series.
     *
     * @return the constant value
     */
    public double getConstantValue() {
        return constantValue;
    }

    /**
     * Factory method to create a FallbackSeriesConstantPlanNode from a FunctionNode.
     * Expects the function node to represent a fallbackSeries function with exactly one numeric argument.
     *
     * @param functionNode the function node representing the fallbackSeries function
     * @return a new FallbackSeriesConstantPlanNode instance
     * @throws IllegalArgumentException if the function node does not have exactly one argument or if the argument is not a valid number
     */
    public static FallbackSeriesConstantPlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (childNodes.size() != 1) {
            throw new IllegalArgumentException("fallbackSeries function expects exactly one argument");
        }
        if (!(childNodes.getFirst() instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("Argument to fallbackSeries function should be a value node");
        }
        double value = Double.parseDouble(valueNode.getValue());
        return new FallbackSeriesConstantPlanNode(M3PlannerContext.generateId(), value);
    }
}
