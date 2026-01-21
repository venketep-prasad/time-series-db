/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.promql.parser.nodes;

import java.util.List;

/**
 * Represents a function call in PromQL.
 * Example: rate(http_requests_total[5m])
 */
public class FunctionCallNode extends PromASTNode {
    /**
     * The function name.
     */
    private final String functionName;

    /**
     * Constructor for FunctionCallNode.
     * @param functionName the function name
     */
    public FunctionCallNode(String functionName) {
        super();
        this.functionName = functionName;
    }

    /**
     * Gets the function name.
     * @return the function name
     */
    public String getFunctionName() {
        return functionName;
    }

    /**
     * Adds an argument to this function call.
     * @param argument the argument to add
     */
    public void addArgument(PromASTNode argument) {
        addChild(argument);
    }

    /**
     * Gets all arguments.
     * @return the list of arguments
     */
    public List<PromASTNode> getArguments() {
        return getChildren();
    }

    @Override
    public <T> T accept(PromASTVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
