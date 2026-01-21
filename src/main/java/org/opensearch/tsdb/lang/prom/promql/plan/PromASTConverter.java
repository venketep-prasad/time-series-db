/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.promql.plan;

import org.opensearch.tsdb.lang.prom.common.AggregationType;
import org.opensearch.tsdb.lang.prom.common.FunctionType;
import org.opensearch.tsdb.lang.prom.promql.parser.nodes.AggregationNode;
import org.opensearch.tsdb.lang.prom.promql.parser.nodes.FunctionCallNode;
import org.opensearch.tsdb.lang.prom.promql.parser.nodes.InstantVectorSelectorNode;
import org.opensearch.tsdb.lang.prom.promql.parser.nodes.LabelMatcherNode;
import org.opensearch.tsdb.lang.prom.promql.parser.nodes.PromASTNode;
import org.opensearch.tsdb.lang.prom.promql.parser.nodes.RangeVectorSelectorNode;
import org.opensearch.tsdb.lang.prom.promql.parser.nodes.RootNode;
import org.opensearch.tsdb.lang.prom.promql.plan.nodes.AggregationPlanNode;
import org.opensearch.tsdb.lang.prom.promql.plan.nodes.FetchPlanNode;
import org.opensearch.tsdb.lang.prom.promql.plan.nodes.FuncPlanNode;
import org.opensearch.tsdb.lang.prom.promql.plan.nodes.PromPlanNode;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Converts PromQL AST to a logical plan.
 */
public class PromASTConverter {
    private static final AtomicInteger ID_GENERATOR = new AtomicInteger(0);

    /**
     * Build a plan from the AST root.
     * @param astRoot the root AST node
     * @return the root plan node
     */
    public PromPlanNode buildPlan(RootNode astRoot) {
        if (astRoot == null || astRoot.getChildren().isEmpty()) {
            throw new IllegalStateException("AST root cannot be null or empty");
        }

        PromASTNode expression = astRoot.getChildren().getFirst();
        return convertNode(expression);
    }

    private PromPlanNode convertNode(PromASTNode node) {
        return switch (node) {
            case AggregationNode aggregationNode -> convertAggregation(aggregationNode);
            case FunctionCallNode functionCallNode -> convertFunctionCall(functionCallNode);
            case RangeVectorSelectorNode rangeVectorSelectorNode -> convertRangeVectorSelector(rangeVectorSelectorNode);
            case InstantVectorSelectorNode instantVectorSelectorNode -> convertInstantVectorSelector(instantVectorSelectorNode);
            default -> throw new IllegalArgumentException("Unsupported AST node type: " + node.getClass().getSimpleName());
        };
    }

    private PromPlanNode convertAggregation(AggregationNode aggNode) {
        // Convert the expression first
        PromPlanNode childPlan = convertNode(aggNode.getExpression());

        // Parse aggregation type
        AggregationType aggType = AggregationType.fromString(aggNode.getAggregationType());

        // Create aggregation plan node
        AggregationPlanNode aggPlanNode = new AggregationPlanNode(
            generateId(),
            aggType,
            aggNode.getGroupingModifier(),
            aggNode.getGroupingLabels()
        );

        aggPlanNode.addChild(childPlan);
        return aggPlanNode;
    }

    private PromPlanNode convertFunctionCall(FunctionCallNode funcNode) {
        String funcName = funcNode.getFunctionName().toLowerCase(Locale.ROOT);

        // Parse function type
        FunctionType funcType;
        try {
            funcType = FunctionType.fromString(funcName);
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException("Function " + funcName + "() is not yet supported");
        }

        // Create function plan node
        FuncPlanNode funcPlanNode = new FuncPlanNode(generateId(), funcType);

        // Validate and convert vector arguments based on function's metadata
        int expectedVectorArgs = funcType.getVectorArgumentCount();
        int actualVectorArgs = funcNode.getArguments().size();

        if (expectedVectorArgs == 0) {
            // Functions like time() and pi() take no arguments
            if (actualVectorArgs != 0) {
                throw new IllegalArgumentException(funcName + "() takes no arguments, but " + actualVectorArgs + " provided");
            }
            // No children to add
        } else if (expectedVectorArgs > 0) {
            // Functions with fixed argument count
            if (actualVectorArgs != expectedVectorArgs) {
                throw new IllegalArgumentException(
                    funcName + "() requires " + expectedVectorArgs + " argument(s), but " + actualVectorArgs + " provided"
                );
            }

            // Convert child expressions
            for (PromASTNode arg : funcNode.getArguments()) {
                PromPlanNode childPlan = convertNode(arg);
                funcPlanNode.addChild(childPlan);
            }
        } else {
            // Variable argument count (not yet supported)
            throw new UnsupportedOperationException(funcName + "() has variable arguments, not yet implemented");
        }

        return funcPlanNode;
    }

    private PromPlanNode convertRangeVectorSelector(RangeVectorSelectorNode rangeNode) {
        FetchPlanNode fetchNode = new FetchPlanNode(generateId(), rangeNode.getMetricName(), rangeNode.getRangeMs());

        // Add label matchers
        for (LabelMatcherNode matcher : rangeNode.getMatchers()) {
            fetchNode.addLabelMatcher(matcher.getLabelName(), matcher.getMatcherType(), matcher.getValue());
        }

        return fetchNode;
    }

    private PromPlanNode convertInstantVectorSelector(InstantVectorSelectorNode vectorNode) {
        FetchPlanNode fetchNode = new FetchPlanNode(
            generateId(),
            vectorNode.getMetricName(),
            null  // No range for instant vector
        );

        // Add label matchers
        for (LabelMatcherNode matcher : vectorNode.getMatchers()) {
            fetchNode.addLabelMatcher(matcher.getLabelName(), matcher.getMatcherType(), matcher.getValue());
        }

        return fetchNode;
    }

    private static int generateId() {
        return ID_GENERATOR.getAndIncrement();
    }

    /**
     * Reset ID generator (useful for testing).
     */
    public static void resetIdGenerator() {
        ID_GENERATOR.set(0);
    }
}
