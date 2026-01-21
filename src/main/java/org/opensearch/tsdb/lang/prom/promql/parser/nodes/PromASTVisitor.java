/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.promql.parser.nodes;

/**
 * Visitor interface for PromQL AST nodes.
 * @param <T> the return type of the visitor methods
 */
public interface PromASTVisitor<T> {
    /**
     * Visits a root node.
     * @param node the root node to visit
     * @return the result of visiting this node
     */
    T visit(RootNode node);

    /**
     * Visits an instant vector selector node.
     * @param node the instant vector selector node to visit
     * @return the result of visiting this node
     */
    T visit(InstantVectorSelectorNode node);

    /**
     * Visits a range vector selector node.
     * @param node the range vector selector node to visit
     * @return the result of visiting this node
     */
    T visit(RangeVectorSelectorNode node);

    /**
     * Visits a function call node.
     * @param node the function call node to visit
     * @return the result of visiting this node
     */
    T visit(FunctionCallNode node);

    /**
     * Visits an aggregation node.
     * @param node the aggregation node to visit
     * @return the result of visiting this node
     */
    T visit(AggregationNode node);

    /**
     * Visits a label matcher node.
     * @param node the label matcher node to visit
     * @return the result of visiting this node
     */
    T visit(LabelMatcherNode node);
}
