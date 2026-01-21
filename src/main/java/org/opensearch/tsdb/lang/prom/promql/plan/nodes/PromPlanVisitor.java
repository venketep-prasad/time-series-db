/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.promql.plan.nodes;

/**
 * Visitor interface for PromQL plan nodes.
 */
public abstract class PromPlanVisitor<T> {

    /**
     * Process a plan node (default implementation).
     * @param node the node to process
     * @return the result of processing
     */
    public T process(PromPlanNode node) {
        return node.accept(this);
    }

    /**
     * Visits a fetch plan node.
     * @param node the fetch node
     * @return the result of visiting
     */
    public abstract T visit(FetchPlanNode node);

    /**
     * Visits a function plan node.
     * @param node the function node
     * @return the result of visiting
     */
    public abstract T visit(FuncPlanNode node);

    /**
     * Visits an aggregation plan node.
     * @param node the aggregation node
     * @return the result of visiting
     */
    public abstract T visit(AggregationPlanNode node);
}
