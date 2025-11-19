/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * HistogramPercentilePlanNode represents a plan node that handles histogram percentile calculations in M3QL.
 * This node calculates percentiles from histogram buckets.
 */
public class HistogramPercentilePlanNode extends M3PlanNode {

    private final String bucketId;
    private final String bucketRange;
    private final List<Float> percentiles;

    /**
     * Constructor for HistogramPercentilePlanNode.
     * @param id The node ID
     * @param bucketId The label name identifying the bucket ID
     * @param bucketRange The label name identifying the bucket range
     * @param percentiles List of percentiles to calculate (0-100)
     */
    public HistogramPercentilePlanNode(int id, String bucketId, String bucketRange, List<Float> percentiles) {
        super(id);
        this.bucketId = bucketId;
        this.bucketRange = bucketRange;
        this.percentiles = new ArrayList<>(percentiles);
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "HISTOGRAM_PERCENTILE(%s, %s, %s)", bucketId, bucketRange, percentiles);
    }

    /**
     * Returns the bucket ID label name.
     * @return The bucket ID label name
     */
    public String getBucketId() {
        return bucketId;
    }

    /**
     * Returns the bucket range label name.
     * @return The bucket range label name
     */
    public String getBucketRange() {
        return bucketRange;
    }

    /**
     * Returns the list of percentiles to calculate.
     * @return List of percentiles (0-100)
     */
    public List<Float> getPercentiles() {
        return new ArrayList<>(percentiles);
    }

    /**
     * Creates a HistogramPercentilePlanNode from a FunctionNode.
     * Expected format: histogramPercentile(bucketId, bucketRange, percentile1, percentile2, ...)
     *
     * @param functionNode The function node to parse
     * @return HistogramPercentilePlanNode instance
     * @throws IllegalArgumentException if the function arguments are invalid
     */
    public static HistogramPercentilePlanNode of(FunctionNode functionNode) {
        if (functionNode.getChildren().size() < 3) {
            throw new IllegalArgumentException(
                "histogramPercentile function must have at least three arguments: bucketId, bucketRange, and percentile(s)"
            );
        }

        // First argument: bucketId
        if (!(functionNode.getChildren().get(0) instanceof ValueNode)) {
            throw new IllegalArgumentException("First argument (bucketId) must be a value");
        }
        String bucketId = ((ValueNode) functionNode.getChildren().get(0)).getValue();

        // Second argument: bucketRange
        if (!(functionNode.getChildren().get(1) instanceof ValueNode)) {
            throw new IllegalArgumentException("Second argument (bucketRange) must be a value");
        }
        String bucketRange = ((ValueNode) functionNode.getChildren().get(1)).getValue();

        // Remaining arguments: percentiles
        List<Float> percentiles = new ArrayList<>();
        for (int i = 2; i < functionNode.getChildren().size(); i++) {
            if (!(functionNode.getChildren().get(i) instanceof ValueNode valueNode)) {
                throw new IllegalArgumentException("Percentile arguments must be values");
            }
            String percentileStr = valueNode.getValue();
            try {
                float percentile = Float.parseFloat(percentileStr);
                if (percentile < 0 || percentile > 100) {
                    throw new IllegalArgumentException("Percentiles must be between 0 and 100 (inclusive), got: " + percentile);
                }
                percentiles.add(percentile);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid percentile value: " + percentileStr, e);
            }
        }

        return new HistogramPercentilePlanNode(M3PlannerContext.generateId(), bucketId, bucketRange, percentiles);
    }
}
