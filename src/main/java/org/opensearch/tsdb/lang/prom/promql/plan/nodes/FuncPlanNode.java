/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.promql.plan.nodes;

import org.opensearch.tsdb.lang.prom.common.FunctionType;

import java.util.ArrayList;
import java.util.List;

/**
 * Plan node representing a PromQL function call.
 *
 * <p>This generic node supports all PromQL functions, making it easy to add
 * new function support without creating new plan node types.
 *
 * <p>Examples:
 * <ul>
 *   <li>rate(http_requests_total[5m])</li>
 *   <li>irate(cpu_usage[1m])</li>
 *   <li>abs(temperature)</li>
 *   <li>histogram_quantile(0.95, rate(request_duration_bucket[5m]))</li>
 * </ul>
 */
public class FuncPlanNode extends PromPlanNode {
    /**
     * The function type.
     */
    private final FunctionType functionType;

    /**
     * Function arguments (e.g., quantile value for histogram_quantile).
     * For most functions, this is empty as the argument is the child node.
     */
    private final List<Object> arguments;

    /**
     * Constructor for FuncPlanNode.
     * @param id unique identifier for this node
     * @param functionType the function type
     */
    public FuncPlanNode(int id, FunctionType functionType) {
        super(id);
        this.functionType = functionType;
        this.arguments = new ArrayList<>();
    }

    /**
     * Constructor with arguments.
     * @param id unique identifier for this node
     * @param functionType the function type
     * @param arguments additional function arguments (scalars, etc.)
     */
    public FuncPlanNode(int id, FunctionType functionType, List<Object> arguments) {
        super(id);
        this.functionType = functionType;
        this.arguments = arguments != null ? new ArrayList<>(arguments) : new ArrayList<>();
    }

    /**
     * Gets the function type.
     * @return the function type
     */
    public FunctionType getFunctionType() {
        return functionType;
    }

    /**
     * Gets the function arguments.
     * @return the list of arguments
     */
    public List<Object> getArguments() {
        return arguments;
    }

    /**
     * Adds an argument to this function.
     * @param argument the argument to add
     */
    public void addArgument(Object argument) {
        arguments.add(argument);
    }

    @Override
    public <T> T accept(PromPlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "Function[" + functionType.getName() + "]";
    }
}
