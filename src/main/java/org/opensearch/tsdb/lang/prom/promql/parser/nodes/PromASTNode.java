/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.promql.parser.nodes;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class representing a node in the PromQL AST.
 */
public abstract class PromASTNode {
    /**
     * The list of child nodes.
     */
    protected final List<PromASTNode> children;

    /**
     * Constructor for PromASTNode.
     */
    protected PromASTNode() {
        this.children = new ArrayList<>();
    }

    /**
     * Adds a child node to this node.
     * @param child the child node to add
     */
    public void addChild(PromASTNode child) {
        children.add(child);
    }

    /**
     * Gets the list of child nodes.
     * @return the list of child nodes
     */
    public List<PromASTNode> getChildren() {
        return children;
    }

    /**
     * Accepts a visitor to perform operations on this node.
     * @param visitor the visitor to accept
     * @return the result of the visitor's operation
     * @param <T> the return type of the visitor's operation
     */
    public abstract <T> T accept(PromASTVisitor<T> visitor);
}
