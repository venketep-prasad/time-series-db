/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.M3Duration;
import org.opensearch.tsdb.lang.m3.common.WindowAggregationType;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.time.Duration;
import java.util.Locale;

/**
 * SummarizePlanNode represents a plan node that handles summarize operations in M3QL.
 * Summarizes the data into interval buckets of a certain size using an aggregation function.
 */
public class SummarizePlanNode extends M3PlanNode {

    /**
     * Go's zero time (0001-01-01 00:00:00 UTC) in milliseconds from Unix epoch.
     * This aligns with Go's time.Time{} for compatibility with M3's time.Truncate() behavior.
     * Used as the default reference time for bucket alignment in M3QL when alignToFrom is false.
     */
    public static final long GO_ZERO_TIME_MILLIS = -62135596800000L;

    private final String interval; // e.g., "5m", "1h"
    private final WindowAggregationType function;
    private final boolean alignToFrom;

    /**
     * Constructor for SummarizePlanNode.
     * @param id node id
     * @param interval the bucket interval (e.g., "5m" for 5 minutes, "1h" for 1 hour)
     * @param function the aggregation function to apply to each bucket
     * @param alignToFrom whether to align buckets to query start time (true) or to interval boundaries (false)
     */
    public SummarizePlanNode(int id, String interval, WindowAggregationType function, boolean alignToFrom) {
        super(id);
        this.interval = interval;
        this.function = function;
        this.alignToFrom = alignToFrom;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "SUMMARIZE(%s, %s, %s)", interval, function, alignToFrom);
    }

    /**
     * Returns the interval duration.
     * @return Duration
     */
    public Duration getInterval() {
        Duration duration = M3Duration.valueOf(interval);
        if (duration.isNegative()) {
            throw new IllegalArgumentException("Interval cannot be negative: " + interval);
        }
        return duration;
    }

    /**
     * Returns the aggregation function for the summarize operation.
     * @return WindowAggregationType aggregation function
     */
    public WindowAggregationType getFunction() {
        return function;
    }

    /**
     * Returns whether to align buckets to query start time.
     * @return boolean true if buckets align to from time, false if aligned to interval boundaries
     */
    public boolean isAlignToFrom() {
        return alignToFrom;
    }

    /**
     * Factory method to create a SummarizePlanNode from a FunctionNode.
     * Expects the function node to represent a SUMMARIZE function with 1-3 arguments:
     * - interval (required): bucket size
     * - function (optional): aggregation function, defaults to "sum" (M3QL default)
     * - alignToFrom (optional): boolean, defaults to false (M3QL default)
     *
     * Note: Default values are set at the M3QL planner level. The SummarizeStage itself
     * requires all parameters to be explicitly specified.
     *
     * @param functionNode the function node representing the SUMMARIZE function
     * @return a new SummarizePlanNode instance
     * @throws IllegalArgumentException if the function node does not have valid arguments
     */
    public static SummarizePlanNode of(FunctionNode functionNode) {
        int argCount = functionNode.getChildren().size();

        if (argCount < 1 || argCount > 3) {
            throw new IllegalArgumentException(
                "Summarize function must have 1-3 arguments: interval, [function], [alignToFrom]. Got: " + argCount
            );
        }

        // Parse interval (first argument)
        if (!(functionNode.getChildren().get(0) instanceof ValueNode intervalNode)) {
            throw new IllegalArgumentException("First argument must be a value representing the interval");
        }
        String interval = intervalNode.getValue();

        // Parse function (second argument, optional, defaults to "sum")
        WindowAggregationType function = WindowAggregationType.SUM; // default
        if (argCount >= 2) {
            if (!(functionNode.getChildren().get(1) instanceof ValueNode functionNode2)) {
                throw new IllegalArgumentException("Second argument must be a value representing the aggregation function");
            }
            String functionStr = functionNode2.getValue();
            function = WindowAggregationType.fromString(functionStr);
        }

        // Parse alignToFrom (third argument, optional, defaults to false)
        boolean alignToFrom = false;
        if (argCount == 3) {
            if (!(functionNode.getChildren().get(2) instanceof ValueNode alignNode)) {
                throw new IllegalArgumentException("Third argument must be a boolean value representing alignToFrom");
            }
            String alignValue = alignNode.getValue().toLowerCase(Locale.ROOT);
            if ("true".equals(alignValue)) {
                alignToFrom = true;
            } else if ("false".equals(alignValue)) {
                alignToFrom = false;
            } else {
                throw new IllegalArgumentException("Third argument (alignToFrom) must be 'true' or 'false', got: " + alignValue);
            }
        }

        return new SummarizePlanNode(M3PlannerContext.generateId(), interval, function, alignToFrom);
    }
}
