/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.promql.plan.nodes;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class representing a node in the PromQL logical plan.
 */
public abstract class PromPlanNode {
    /**
     * Unique identifier for this plan node.
     */
    private final int id;

    /**
     * List of child plan nodes.
     */
    protected final List<PromPlanNode> children;

    /**
     * Constructor for PromPlanNode.
     * @param id unique identifier for this node
     */
    protected PromPlanNode(int id) {
        this.id = id;
        this.children = new ArrayList<>();
    }

    /**
     * Gets the unique identifier of this plan node.
     * @return the unique plan node ID
     */
    public int getId() {
        return id;
    }

    /**
     * Adds a child plan node.
     * @param child the child node to add
     */
    public void addChild(PromPlanNode child) {
        children.add(child);
    }

    /**
     * Gets the list of child plan nodes.
     * @return the list of children
     */
    public List<PromPlanNode> getChildren() {
        return children;
    }

    /**
     * Accepts a visitor to perform operations on this node.
     * @param visitor the visitor to accept
     * @return the result of the visitor's operation
     * @param <T> the return type of the visitor's operation
     */
    public abstract <T> T accept(PromPlanVisitor<T> visitor);

    /**
     * Gets a human-readable name for this plan node.
     * @return a descriptive name for this node type
     */
    public abstract String getExplainName();
}
