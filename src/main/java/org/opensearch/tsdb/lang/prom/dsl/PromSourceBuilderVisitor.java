/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.dsl;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.LabelConstants;
import org.opensearch.tsdb.lang.prom.common.AggregationType;
import org.opensearch.tsdb.lang.prom.common.FunctionType;
import org.opensearch.tsdb.lang.prom.common.GroupingModifier;
import org.opensearch.tsdb.lang.prom.common.MatcherType;
import org.opensearch.tsdb.lang.prom.promql.plan.nodes.AggregationPlanNode;
import org.opensearch.tsdb.lang.prom.promql.plan.nodes.FetchPlanNode;
import org.opensearch.tsdb.lang.prom.promql.plan.nodes.FuncPlanNode;
import org.opensearch.tsdb.lang.prom.promql.plan.nodes.PromPlanNode;
import org.opensearch.tsdb.lang.prom.promql.plan.nodes.PromPlanVisitor;
import org.opensearch.tsdb.lang.m3.stage.AvgStage;
import org.opensearch.tsdb.lang.m3.stage.CountStage;
import org.opensearch.tsdb.lang.m3.stage.MaxStage;
import org.opensearch.tsdb.lang.m3.stage.MinStage;
import org.opensearch.tsdb.lang.m3.stage.PerSecondRateStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregator;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;
import org.opensearch.tsdb.query.search.CachedWildcardQueryBuilder;
import org.opensearch.tsdb.query.search.TimeRangePruningQueryBuilder;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * Visitor that converts PromQL plan nodes to OpenSearch Query DSL components.
 */
public class PromSourceBuilderVisitor extends PromPlanVisitor<PromSourceBuilderVisitor.ComponentHolder> {
    private static final String UNFOLD_NAME_SUFFIX = "_unfold";
    private static final String COORDINATOR_NAME_SUFFIX = "_coordinator";
    private static final String NAME = "__name__";
    private static final LinkedHashMap<String, TimeSeriesCoordinatorAggregator.MacroDefinition> EMPTY_MAP = new LinkedHashMap<>();

    private final Stack<UnaryPipelineStage> stageStack;
    private final PromOSTranslator.Params params;
    private long lookback; // Lookback duration in ms for range vector selectors

    public PromSourceBuilderVisitor(PromOSTranslator.Params params) {
        this.params = params;
        this.stageStack = new Stack<>();
        this.lookback = 0;
    }

    @Override
    public ComponentHolder visit(FetchPlanNode node) {
        String unfoldName = node.getId() + UNFOLD_NAME_SUFFIX;

        if (node.isRangeVector()) {
            lookback = node.getRangeMs();
        } else {
            lookback = params.lookbackDelta();  // Use lookback delta
        }

        // Build query for fetch with adjusted time range (extending backwards for lookback)
        // The unfold aggregator will naturally filter to [adjustedStart, adjustedEnd) via decodeSamples()
        long adjustedStartTime = params.startTime() - lookback;
        long adjustedEndTime = params.endTime();
        QueryBuilder query = buildQueryForFetch(node, adjustedStartTime, adjustedEndTime);

        // Collect stages for unfold aggregation (shard-level processing)
        List<UnaryPipelineStage> unfoldStages = new ArrayList<>();
        if (params.pushdown()) {
            // Add stages that can be pushed down to shard level
            while (!stageStack.isEmpty() && !stageStack.peek().isCoordinatorOnly()) {
                unfoldStages.add(stageStack.pop());
            }
        }

        // Create unfold aggregation builder with adjusted time range
        // The minTimestamp and maxTimestamp control:
        // 1. decodeSamples() filtering - what samples to extract from chunks
        // 2. TimeSeries bounds - the min/max timestamp of the resulting time series
        // TODO: add TruncateStage for aggregation_over_time functions
        TimeSeriesUnfoldAggregationBuilder unfoldBuilder = new TimeSeriesUnfoldAggregationBuilder(
            unfoldName,
            unfoldStages,
            adjustedStartTime,
            adjustedEndTime,
            params.step()
        );

        ComponentHolder holder = new ComponentHolder(node.getId());
        holder.setQuery(query);
        holder.setUnfoldAggregationBuilder(unfoldBuilder);

        // Add remaining stages to coordinator aggregation
        if (!stageStack.isEmpty()) {
            List<PipelineStage> coordinatorStages = new ArrayList<>();
            while (!stageStack.isEmpty()) {
                coordinatorStages.add(stageStack.pop());
            }

            holder.addPipelineAggregationBuilder(
                new TimeSeriesCoordinatorAggregationBuilder(
                    node.getId() + COORDINATOR_NAME_SUFFIX,
                    coordinatorStages,
                    EMPTY_MAP,
                    Map.of(unfoldName, unfoldName),
                    unfoldName
                )
            );
        }

        return holder;
    }

    @Override
    public ComponentHolder visit(FuncPlanNode node) {
        // Validate argument count
        int expectedVectorArgs = node.getFunctionType().getVectorArgumentCount();
        int actualVectorArgs = node.getChildren().size();

        if (expectedVectorArgs >= 0 && actualVectorArgs != expectedVectorArgs) {
            throw new IllegalArgumentException(
                node.getFunctionType().getName() + "() expects " + expectedVectorArgs + " vector argument(s), got " + actualVectorArgs
            );
        }

        // Handle functions with zero children (constants)
        if (actualVectorArgs == 0) {
            switch (node.getFunctionType()) {
                case TIME, PI -> throw new UnsupportedOperationException(
                    "Function " + node.getFunctionType().getName() + "() is not yet implemented"
                );
                default -> throw new IllegalStateException(node.getFunctionType().getName() + "() should have at least one argument");
            }
        }

        // Most functions have one child - process it
        PromPlanNode child = node.getChildren().getFirst();

        // Handle different function types
        switch (node.getFunctionType()) {
            case RATE -> {
                FetchPlanNode fetchNode = requireRangeVector(child, node.getFunctionType());

                // Add PerSecondRateStage to the stack
                // Use the range from the range vector selector as the interval
                long interval = fetchNode.getRangeMs();
                // TODO: use m3 stage for now
                PerSecondRateStage rateStage = new PerSecondRateStage(interval, 1000); // 1000ms per second
                stageStack.push(rateStage);
            }
            case INCREASE -> {
                FetchPlanNode fetchNode = requireRangeVector(child, node.getFunctionType());

                // For now, treat increase similar to rate
                // TODO: Implement proper increase calculation (without per-second division)
                long interval = fetchNode.getRangeMs();
                PerSecondRateStage rateStage = new PerSecondRateStage(interval, 1); // No per-second normalization
                stageStack.push(rateStage);
            }
            default -> throw new UnsupportedOperationException(
                "Function " + node.getFunctionType().getName() + "() is not yet implemented"
            );
        }

        // Process the child fetch node
        return child.accept(this);
    }

    /**
     * Validates that the child node is a FetchPlanNode with a range vector.
     *
     * @param child the child plan node to validate
     * @param functionType the function type requiring a range vector
     * @return the validated FetchPlanNode
     * @throws IllegalArgumentException if validation fails
     */
    private FetchPlanNode requireRangeVector(PromPlanNode child, FunctionType functionType) {
        if (!(child instanceof FetchPlanNode fetchNode)) {
            throw new IllegalArgumentException(
                functionType.getName() + "() requires a range vector, but got " + child.getClass().getSimpleName()
            );
        }

        if (!fetchNode.isRangeVector()) {
            throw new IllegalArgumentException(
                functionType.getName() + "() requires a range vector with [duration], " + "but got an instant vector"
            );
        }

        return fetchNode;
    }

    /**
     * Validates that the child node is a FetchPlanNode with an instant vector.
     *
     * @param child the child plan node to validate
     * @param functionType the function type requiring an instant vector
     * @return the validated FetchPlanNode
     * @throws IllegalArgumentException if validation fails
     */
    private FetchPlanNode requireInstantVector(PromPlanNode child, FunctionType functionType) {
        if (!(child instanceof FetchPlanNode fetchNode)) {
            throw new IllegalArgumentException(
                functionType.getName() + "() requires an instant vector, but got " + child.getClass().getSimpleName()
            );
        }

        if (fetchNode.isRangeVector()) {
            throw new IllegalArgumentException(
                functionType.getName() + "() requires an instant vector, " + "but got a range vector with [duration]"
            );
        }

        return fetchNode;
    }

    @Override
    public ComponentHolder visit(AggregationPlanNode node) {
        if (node.getChildren().size() != 1) {
            throw new IllegalStateException("Aggregation node must have exactly one child");
        }

        // Determine which labels to group by
        List<String> groupByLabels = determineGroupByLabels(node);

        // Create appropriate stage based on aggregation type
        UnaryPipelineStage aggStage = createAggregationStage(node.getAggregationType(), groupByLabels);
        stageStack.push(aggStage);

        // Process the child expression
        return node.getChildren().getFirst().accept(this);
    }

    private List<String> determineGroupByLabels(AggregationPlanNode node) {
        if (node.getGroupingModifier() == null) {
            // No grouping specified - aggregate all together
            return Collections.emptyList();
        }

        if (node.getGroupingModifier() == GroupingModifier.BY) {
            // Keep only specified labels
            return node.getGroupingLabels();
        } else {
            // WITHOUT - not supported in this basic implementation
            throw new UnsupportedOperationException("WITHOUT modifier not yet supported");
        }
    }

    private UnaryPipelineStage createAggregationStage(AggregationType type, List<String> groupByLabels) {
        return switch (type) {
            case SUM -> groupByLabels.isEmpty() ? new SumStage() : new SumStage(groupByLabels);
            case AVG -> groupByLabels.isEmpty() ? new AvgStage() : new AvgStage(groupByLabels);
            case MIN -> groupByLabels.isEmpty() ? new MinStage() : new MinStage(groupByLabels);
            case MAX -> groupByLabels.isEmpty() ? new MaxStage() : new MaxStage(groupByLabels);
            case COUNT -> groupByLabels.isEmpty() ? new CountStage() : new CountStage(groupByLabels);
        };
    }

    private QueryBuilder buildQueryForFetch(FetchPlanNode node, long startTime, long endTime) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        // Add time range filter
        boolQuery.filter(QueryBuilders.rangeQuery(Constants.IndexSchema.TIMESTAMP_RANGE).gte(startTime).lt(endTime));

        // Add metric name filter if specified
        if (node.getMetricName() != null) {
            boolQuery.filter(
                QueryBuilders.termsQuery(Constants.IndexSchema.LABELS, NAME + LabelConstants.LABEL_DELIMITER + node.getMetricName())
            );
        }

        // Add label matchers
        for (FetchPlanNode.LabelMatcherInfo matcher : node.getLabelMatchers().values()) {
            QueryBuilder matcherQuery = buildLabelMatcherQuery(matcher);

            if (matcher.getMatcherType() == MatcherType.NOT_EQUAL || matcher.getMatcherType() == MatcherType.REGEX_NOT_MATCH) {
                boolQuery.mustNot(matcherQuery);
            } else {
                boolQuery.filter(matcherQuery);
            }
        }

        // Wrap with time range pruning query
        return new TimeRangePruningQueryBuilder(boolQuery, startTime, endTime);
    }

    private QueryBuilder buildLabelMatcherQuery(FetchPlanNode.LabelMatcherInfo matcher) {
        String labelFilter = matcher.getLabelName() + LabelConstants.LABEL_DELIMITER + matcher.getValue();

        return switch (matcher.getMatcherType()) {
            case EQUAL, NOT_EQUAL -> QueryBuilders.termsQuery(Constants.IndexSchema.LABELS, labelFilter);
            case REGEX_MATCH, REGEX_NOT_MATCH -> new CachedWildcardQueryBuilder(
                Constants.IndexSchema.LABELS,
                convertRegexToWildcard(labelFilter)
            );
        };
    }

    /**
     * Convert PromQL regex to wildcard pattern (simplified).
     * In a full implementation, this would need proper regex support.
     */
    private String convertRegexToWildcard(String labelFilter) {
        // For basic support, treat .* as *
        return labelFilter.replace(".*", "*");
    }

    /**
     * Holds query components during plan traversal.
     */
    public static class ComponentHolder {
        private final int id;
        private QueryBuilder query;
        private TimeSeriesUnfoldAggregationBuilder unfoldAggregationBuilder;
        private final List<TimeSeriesCoordinatorAggregationBuilder> pipelineAggregationBuilders;

        public ComponentHolder(int id) {
            this.id = id;
            this.pipelineAggregationBuilders = new ArrayList<>();
        }

        public int getId() {
            return id;
        }

        public void setQuery(QueryBuilder query) {
            this.query = query;
        }

        public void setUnfoldAggregationBuilder(TimeSeriesUnfoldAggregationBuilder builder) {
            this.unfoldAggregationBuilder = builder;
        }

        public void addPipelineAggregationBuilder(TimeSeriesCoordinatorAggregationBuilder builder) {
            this.pipelineAggregationBuilders.add(builder);
        }

        public SearchSourceBuilder toSearchSourceBuilder() {
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.size(0);
            builder.query(query);

            if (unfoldAggregationBuilder != null) {
                builder.aggregation(unfoldAggregationBuilder);
            }

            for (TimeSeriesCoordinatorAggregationBuilder pipelineAgg : pipelineAggregationBuilders) {
                builder.aggregation(pipelineAgg);
            }

            return builder;
        }
    }
}
