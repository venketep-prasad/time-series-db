/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.MultiValueSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.ParallelProcessingConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Abstract base class for pipeline stages that support label grouping and calculation for each Sample.
 * Provides common functionality for grouping time series by labels and applying
 * aggregation functions within each group for each sample.
 *
 * @param <A> The type of class used as aggregation bucket, concrete class typically should specify this type
 *
 * <p>This implementation supports both sequential and parallel processing modes:</p>
 * <ul>
 *   <li><strong>Sequential:</strong> Used for small datasets (&lt; 1000 series) to avoid thread overhead</li>
 *   <li><strong>Parallel:</strong> Used for large datasets to leverage multi-core CPUs at coordinator level</li>
 * </ul>
 *
 * <p>Parallel processing uses thread-local aggregation then merges
 * results, with work executed via {@link java.util.concurrent.ForkJoinPool} common pool.</p>
 */
public abstract class AbstractGroupingSampleStage<A> extends AbstractGroupingStage {

    private static final Logger logger = LogManager.getLogger(AbstractGroupingSampleStage.class);

    /**
     * Configuration for parallel processing thresholds in grouping stages.
     * Uses default config since stages don't have access to cluster settings.
     * Can be overridden via setParallelConfig for testing.
     */
    private static volatile ParallelProcessingConfig parallelConfig = ParallelProcessingConfig.defaultConfig();

    /**
     * Constructor for aggregation without label grouping.
     */
    protected AbstractGroupingSampleStage() {
        super();
    }

    /**
     * Constructor for aggregation with label grouping.
     * @param groupByLabels List of label names to group by. TimeSeries with the same values for these labels will be aggregated together.
     */
    protected AbstractGroupingSampleStage(List<String> groupByLabels) {
        super(groupByLabels);
    }

    /**
     * Constructor for aggregation with single label grouping.
     * @param groupByLabel Single label name to group by.
     */
    protected AbstractGroupingSampleStage(String groupByLabel) {
        super(groupByLabel);
    }

    /**
     * Set the parallel processing configuration for grouping stages.
     * Primarily intended for testing to control parallel vs sequential execution.
     *
     * @param config the parallel processing configuration to use
     */
    public static void setParallelConfig(ParallelProcessingConfig config) {
        parallelConfig = config;
    }

    /**
     * Get the current parallel processing configuration for grouping stages.
     *
     * @return the current configuration
     */
    public static ParallelProcessingConfig getParallelConfig() {
        return parallelConfig;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        return processWithContext(input, true, null);
    }

    /**
     * Aggregate a single sample into the bucket
     *
     * @param bucket could be null if the sample is the first sample of this particular timestamp
     * @param newSample new sample that will be aggregated to the bucket
     * @return The bucket after aggregation, could be the original one or a newly created one
     */
    protected abstract A aggregateSingleSample(@Nullable A bucket, Sample newSample);

    /**
     * Convert the bucket back to {@link Sample} so that it can be put back to {@link TimeSeries}
     *
     * @return a newly constructed {@link Sample}, or it could be the bucket itself if it is already the sample,
     * like {@link org.opensearch.tsdb.core.model.SumCountSample}
     */
    protected abstract Sample bucketToSample(long timestamp, A bucket);

    /**
     * Process a group of time series using the template method pattern.
     * This method handles the common aggregation logic while delegating
     * operation-specific behavior to abstract methods.
     *
     * <p>Automatically selects sequential or parallel processing based on dataset size.</p>
     *
     * @param groupSeries List of time series in the same group
     * @param groupLabels The labels for this group (null if no grouping)
     * @return Single processed time series for this group
     */
    @Override
    protected final TimeSeries processGroup(List<TimeSeries> groupSeries, Labels groupLabels) {
        if (groupSeries.isEmpty()) {
            throw new IllegalArgumentException("groupSeries must not be empty");
        }
        TimeSeries firstSeries = groupSeries.get(0);
        int seriesCount = groupSeries.size();

        int totalSamples = 0;
        for (TimeSeries series : groupSeries) {
            totalSamples += series.getSamples().size();
        }
        int avgSamplesPerSeries = totalSamples / seriesCount;

        // Determine if parallel processing should be used
        boolean useParallel = parallelConfig.shouldUseParallelProcessing(seriesCount, avgSamplesPerSeries);

        if (useParallel) {
            logger.debug(
                "Using parallel processing for stage={}, seriesCount={}, avgSamplesPerSeries={}",
                getName(),
                seriesCount,
                avgSamplesPerSeries
            );
            return processGroupParallel(groupSeries, groupLabels, firstSeries);
        } else {
            logger.debug(
                "Using sequential processing for stage={}, seriesCount={}, avgSamplesPerSeries={}",
                getName(),
                seriesCount,
                avgSamplesPerSeries
            );
            return processGroupSequential(groupSeries, groupLabels, firstSeries);
        }
    }

    /**
     * Process a group of time series sequentially (original implementation).
     * Used for small datasets where thread overhead is not justified.
     *
     * @param groupSeries List of time series in the same group
     * @param groupLabels The labels for this group (null if no grouping)
     * @param firstSeries The first time series (for metadata extraction)
     * @return Single processed time series for this group
     */
    private TimeSeries processGroupSequential(List<TimeSeries> groupSeries, Labels groupLabels, TimeSeries firstSeries) {
        // Calculate expected number of unique timestamps based on time range and step
        long timeRange = firstSeries.getMaxTimestamp() - firstSeries.getMinTimestamp();
        int expectedTimestamps = (int) (timeRange / firstSeries.getStep()) + 1;

        // TODO: This pre-allocation assumes all time series are well-aligned with the same step size.
        // Need to revisit if we want to support multi-resolution queries where different time series
        // may have different step sizes or misaligned timestamps. In such cases, the calculation
        // would need to account for the union of all possible timestamps across all series.

        // Aggregate samples by timestamp using operation-specific logic
        // Pre-allocate HashMap based on expected number of timestamps
        Map<Long, A> timestampToAggregated = HashMap.newHashMap(expectedTimestamps);

        for (TimeSeries series : groupSeries) {
            aggregateSamplesIntoMap(series.getSamples(), timestampToAggregated);
        }
        // Create sorted samples - pre-allocate since we know the exact size
        List<Sample> aggregatedSamples = new ArrayList<>(timestampToAggregated.size());
        // TODO: We could do (slightly) better here in theory -- this is an O(N * log(N)) sort
        // if we do k-way merge in above instead of using an HashMap, then it will be an O(N * log(k))
        // algorithm, tho it's only slightly better
        timestampToAggregated.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> aggregatedSamples.add(bucketToSample(entry.getKey(), entry.getValue())));

        // Assumption: All time series in a group have the same metadata (start time, end time, step)
        // The result will inherit metadata from the first time series in the group
        // TODO: Support misaligned time series inputs if there are real needs

        // Return a single time series with the provided labels
        return new TimeSeries(
            aggregatedSamples,
            groupLabels != null ? groupLabels : ByteLabels.emptyLabels(),
            firstSeries.getMinTimestamp(),
            firstSeries.getMaxTimestamp(),
            firstSeries.getStep(),
            firstSeries.getAlias()
        );
    }

    /**
     * Process a group of time series in parallel using ForkJoinPool.
     * Used for large datasets to leverage multi-core CPUs.
     *
     * <p>Implementation notes:</p>
     * <ul>
     *   <li>Each thread aggregates a subset of series into a local map (no lock contention)</li>
     *   <li>Local maps are then reduced by merging buckets per timestamp via {@link #mergeBuckets}</li>
     *   <li>Uses parallel streams backed by {@link java.util.concurrent.ForkJoinPool#commonPool()}</li>
     * </ul>
     *
     * @param groupSeries List of time series in the same group
     * @param groupLabels The labels for this group (null if no grouping)
     * @param firstSeries The first time series (for metadata extraction)
     * @return Single processed time series for this group
     */
    private TimeSeries processGroupParallel(List<TimeSeries> groupSeries, Labels groupLabels, TimeSeries firstSeries) {
        long timeRange = firstSeries.getMaxTimestamp() - firstSeries.getMinTimestamp();
        int expectedTimestamps = (int) (timeRange / firstSeries.getStep()) + 1;

        // Each thread aggregates into a local map, then we merge (avoids per-key contention)
        Map<Long, A> timestampToAggregated = groupSeries.parallelStream().map(series -> {
            Map<Long, A> local = HashMap.newHashMap(expectedTimestamps);
            aggregateSamplesIntoMap(series.getSamples(), local);
            return local;
        }).reduce(new HashMap<>(), this::mergeLocalMaps);

        // Create sorted samples - pre-allocate since we know the exact size
        List<Sample> aggregatedSamples = new ArrayList<>(timestampToAggregated.size());
        timestampToAggregated.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> aggregatedSamples.add(bucketToSample(entry.getKey(), entry.getValue())));

        // Return a single time series with the provided labels
        return new TimeSeries(
            aggregatedSamples,
            groupLabels != null ? groupLabels : ByteLabels.emptyLabels(),
            firstSeries.getMinTimestamp(),
            firstSeries.getMaxTimestamp(),
            firstSeries.getStep(),
            firstSeries.getAlias()
        );
    }

    @Override
    protected final InternalAggregation reduceGrouped(
        List<TimeSeriesProvider> aggregations,
        TimeSeriesProvider firstAgg,
        TimeSeries firstTimeSeries,
        boolean isFinalReduce
    ) {
        // Calculate total series count to determine if parallel processing should be used
        int totalSeriesCount = 0;
        int totalSamples = 0;
        for (TimeSeriesProvider agg : aggregations) {
            for (TimeSeries ts : agg.getTimeSeries()) {
                totalSeriesCount++;
                totalSamples += ts.getSamples().size();
            }
        }
        int avgSamplesPerSeries = (totalSeriesCount == 0) ? 0 : (totalSamples / totalSeriesCount);

        boolean useParallel = parallelConfig.shouldUseParallelProcessing(totalSeriesCount, avgSamplesPerSeries);

        if (useParallel) {
            logger.debug(
                "Using parallel reduce for stage={}, totalSeries={}, avgSamples={}",
                getName(),
                totalSeriesCount,
                avgSamplesPerSeries
            );
            return reduceGroupedParallel(aggregations, firstAgg, firstTimeSeries, isFinalReduce);
        } else {
            logger.debug(
                "Using sequential reduce for stage={}, totalSeries={}, avgSamples={}",
                getName(),
                totalSeriesCount,
                avgSamplesPerSeries
            );
            return reduceGroupedSequential(aggregations, firstAgg, firstTimeSeries, isFinalReduce);
        }
    }

    /**
     * Sequential reduce implementation (original logic).
     */
    private InternalAggregation reduceGroupedSequential(
        List<TimeSeriesProvider> aggregations,
        TimeSeriesProvider firstAgg,
        TimeSeries firstTimeSeries,
        boolean isFinalReduce
    ) {
        // Combine samples by group across all aggregations
        Map<ByteLabels, Map<Long, A>> groupToTimestampBucket = new HashMap<>();

        for (TimeSeriesProvider aggregation : aggregations) {
            for (TimeSeries series : aggregation.getTimeSeries()) {
                // For global case (no grouping), use empty labels
                ByteLabels groupLabels = extractGroupLabelsDirect(series);
                Map<Long, A> timestampToBucket = groupToTimestampBucket.computeIfAbsent(groupLabels, k -> new HashMap<>());

                // Aggregate samples for this series into the group's timestamp map
                aggregateSamplesIntoMap(series.getSamples(), timestampToBucket);
            }
        }

        return finalizeReduction(groupToTimestampBucket, firstAgg, firstTimeSeries, isFinalReduce);
    }

    /**
     * Parallel reduce implementation using thread-local aggregation then merge (same pattern as
     * {@link #processGroupParallel}). Each thread aggregates one or more aggregations into a local
     * {@code Map<ByteLabels, Map<Long, A>>}, then partial results are combined via {@link #mergeGroupMaps}.
     * Avoids per-key contention by not sharing maps across threads.
     */
    private InternalAggregation reduceGroupedParallel(
        List<TimeSeriesProvider> aggregations,
        TimeSeriesProvider firstAgg,
        TimeSeries firstTimeSeries,
        boolean isFinalReduce
    ) {
        Map<ByteLabels, Map<Long, A>> groupToTimestampBucket = aggregations.parallelStream().map(aggregation -> {
            Map<ByteLabels, Map<Long, A>> local = new HashMap<>();
            for (TimeSeries series : aggregation.getTimeSeries()) {
                ByteLabels groupLabels = extractGroupLabelsDirect(series);
                Map<Long, A> timestampToBucket = local.computeIfAbsent(groupLabels, k -> new HashMap<>());
                aggregateSamplesIntoMap(series.getSamples(), timestampToBucket);
            }
            return local;
        }).reduce(new HashMap<>(), this::mergeGroupMaps);

        return finalizeReduction(groupToTimestampBucket, firstAgg, firstTimeSeries, isFinalReduce);
    }

    /**
     * Merge two group→timestamp maps (used when reducing parallel reduce partials).
     * Must not mutate either argument so the combiner is safe when the same identity is
     * reused in parallel (stream reduce can call combine(identity, p1) and combine(identity, p2)
     * in different threads, then combine those results).
     */
    private Map<ByteLabels, Map<Long, A>> mergeGroupMaps(Map<ByteLabels, Map<Long, A>> a, Map<ByteLabels, Map<Long, A>> b) {
        if (a.isEmpty()) {
            return b;
        }
        if (b.isEmpty()) {
            return a;
        }
        // Copy a so we never mutate the shared identity; then merge b into the copy
        Map<ByteLabels, Map<Long, A>> result = new HashMap<>();
        for (Entry<ByteLabels, Map<Long, A>> e : a.entrySet()) {
            result.put(e.getKey(), new HashMap<>(e.getValue()));
        }
        for (Entry<ByteLabels, Map<Long, A>> e : b.entrySet()) {
            Map<Long, A> resultTs = result.computeIfAbsent(e.getKey(), k -> new HashMap<>());
            for (Entry<Long, A> be : e.getValue().entrySet()) {
                resultTs.compute(be.getKey(), (ts, x) -> mergeBuckets(x, be.getValue(), ts));
            }
        }
        return result;
    }

    /**
     * Finalize the reduction by creating time series from aggregated buckets.
     * Shared logic for both sequential and parallel paths.
     */
    private InternalAggregation finalizeReduction(
        Map<ByteLabels, Map<Long, A>> groupToTimestampBucket,
        TimeSeriesProvider firstAgg,
        TimeSeries firstTimeSeries,
        boolean isFinalReduce
    ) {
        // Create the final aggregated time series for each group
        // Pre-allocate result list since we know exactly how many groups we have
        List<TimeSeries> resultTimeSeries = new ArrayList<>(groupToTimestampBucket.size());

        for (Map.Entry<ByteLabels, Map<Long, A>> entry : groupToTimestampBucket.entrySet()) {
            ByteLabels groupLabels = entry.getKey();
            Map<Long, A> timestampToBucket = entry.getValue();

            // Pre-allocate samples list since we know exactly how many timestamps we have
            List<Sample> samples = new ArrayList<>(timestampToBucket.size());
            timestampToBucket.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(bucketEntry -> {
                // Convert bucket to sample
                samples.add(bucketToSample(bucketEntry.getKey(), bucketEntry.getValue()));
            });

            Labels finalLabels = groupLabels.isEmpty() ? ByteLabels.emptyLabels() : groupLabels;

            // Use metadata from the first nonEmpty time series
            resultTimeSeries.add(
                new TimeSeries(
                    samples,
                    finalLabels,
                    firstTimeSeries.getMinTimestamp(),
                    firstTimeSeries.getMaxTimestamp(),
                    firstTimeSeries.getStep(),
                    firstTimeSeries.getAlias()
                )
            );
        }

        // Apply sample materialization if this is the final reduce phase and materialization is needed
        if (isFinalReduce && needsMaterialization()) {
            for (int i = 0; i < resultTimeSeries.size(); i++) {
                resultTimeSeries.set(i, materializeSamples(resultTimeSeries.get(i)));
            }
        }

        TimeSeriesProvider result = firstAgg.createReduced(resultTimeSeries);
        return (InternalAggregation) result;
    }

    /**
     * Merge two aggregation buckets for the same timestamp (used when reducing thread-local maps).
     * Default: treat the second bucket as a single sample and aggregate into the first.
     * Override in stages where buckets must be combined differently (e.g. percentile sorted lists).
     *
     * @param existing current bucket for the timestamp (null if none yet)
     * @param toMerge bucket from another thread to merge in
     * @param timestamp the timestamp (for sample conversion when needed)
     * @return merged bucket
     */
    protected A mergeBuckets(@Nullable A existing, A toMerge, long timestamp) {
        return aggregateSingleSample(existing, bucketToSample(timestamp, toMerge));
    }

    /**
     * Merge two thread-local timestamp→bucket maps into one. Does not mutate either argument
     * so the combiner is safe when used with parallel stream reduce (identity may be reused).
     */
    private Map<Long, A> mergeLocalMaps(Map<Long, A> a, Map<Long, A> b) {
        if (a.isEmpty()) {
            return b;
        }
        if (b.isEmpty()) {
            return a;
        }
        Map<Long, A> result = new HashMap<>(a);
        for (Entry<Long, A> e : b.entrySet()) {
            result.compute(e.getKey(), (ts, x) -> mergeBuckets(x, e.getValue(), ts));
        }
        return result;
    }

    /**
     * Helper method to aggregate samples into an existing timestamp map.
     */
    private void aggregateSamplesIntoMap(SampleList samples, Map<Long, A> timestampToSample) {
        for (Sample sample : samples) {
            // Skip NaN values - treat them as null/missing (MultiValueSample does not support getValue())
            if (!(sample instanceof MultiValueSample) && Double.isNaN(sample.getValue())) {
                continue;
            }
            long timestamp = sample.getTimestamp();

            timestampToSample.compute(timestamp, (ts, a) -> aggregateSingleSample(a, sample));
        }
    }

    /**
     * Common writeTo implementation for all grouping stages.
     */
    public void writeTo(StreamOutput out) throws IOException {
        // Write groupByLabels information
        List<String> groupByLabels = getGroupByLabels();
        if (!groupByLabels.isEmpty()) {
            out.writeBoolean(true);
            out.writeStringCollection(groupByLabels);
        } else {
            out.writeBoolean(false);
        }
    }

    /**
     * Common isGlobalAggregation implementation for all grouping stages.
     */
    public boolean isGlobalAggregation() {
        return true;
    }

    /**
     * Get all groupByLabels (for multi-label grouping).
     * @return the list of groupByLabels, or empty list if no grouping
     */
    public List<String> getGroupByLabels() {
        return groupByLabels;
    }
}
