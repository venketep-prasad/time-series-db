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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.MultiInputPipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.utils.SampleMerger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Multi-input pipeline stage that stitches time series from multiple sub-queries
 * based on their time ranges.
 *
 * <p>This stage is designed for scenarios where a single query is split into multiple
 * sub-queries (e.g., querying different time ranges or different clusters), and the
 * results need to be combined back into continuous time series.</p>
 *
 * <h2>Stitching Logic:</h2>
 * <ul>
 *   <li>Groups time series from all sub-queries by their labels</li>
 *   <li>For each label group, merges samples from all sub-queries in time order</li>
 *   <li>Handles overlapping timestamps using configurable deduplication policy</li>
 *   <li>Preserves time series metadata (min/max timestamps, step)</li>
 * </ul>
 *
 * <h2>Sub-Query Metadata:</h2>
 * <p>Each sub-query has associated metadata that includes:
 * <ul>
 *   <li><strong>Reference Name:</strong> Identifier for the sub-query (e.g., "R1", "R2")</li>
 *   <li><strong>Stitch Start:</strong> Start timestamp of this sub-query's time range</li>
 *   <li><strong>Stitch End:</strong> End timestamp of this sub-query's time range</li>
 * </ul>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * // Create stitch metadata for two sub-queries
 * Map<String, StitchMetadata> metadata = Map.of(
 *     "R1", new StitchMetadata("R1", 1000L, 2000L),
 *     "R2", new StitchMetadata("R2", 2000L, 3000L)
 * );
 *
 * // Create stitch stage
 * StitchStage stage = new StitchStage(metadata);
 *
 * // Process inputs
 * Map<String, List<TimeSeries>> inputs = Map.of(
 *     "R1", timeSeriesFromR1,
 *     "R2", timeSeriesFromR2
 * );
 * List<TimeSeries> stitched = stage.process(inputs);
 * }</pre>
 *
 * <h2>Performance Considerations:</h2>
 * <ul>
 *   <li>Groups time series by labels using HashMap (O(n) for n total series)</li>
 *   <li>Merges samples using SampleMerger's efficient algorithms</li>
 *   <li>Memory overhead scales with number of unique label combinations</li>
 * </ul>
 */
@PipelineStageAnnotation(name = StitchStage.NAME)
public class StitchStage implements MultiInputPipelineStage {

    private static final Logger logger = LogManager.getLogger(StitchStage.class);

    /** The name of this pipeline stage. */
    public static final String NAME = "_stitch"; // Internal stage, use underscore prefix

    /** Parameter key for sub-query metadata in serialization */
    private static final String SUB_QUERIES_PARAM_KEY = "sub_queries";

    /**
     * Metadata for a single sub-query in the stitch operation.
     */
    public static class StitchMetadata {
        private final String referenceName;
        private final long stitchStart;
        private final long stitchEnd;

        /**
         * Create stitch metadata for a sub-query.
         *
         * @param referenceName Reference name for this sub-query (e.g., "R1", "R2")
         * @param stitchStart Start timestamp of the time range (inclusive)
         * @param stitchEnd End timestamp of the time range (inclusive)
         */
        public StitchMetadata(String referenceName, long stitchStart, long stitchEnd) {
            this.referenceName = Objects.requireNonNull(referenceName, "Reference name cannot be null");
            this.stitchStart = stitchStart;
            this.stitchEnd = stitchEnd;

            if (stitchStart > stitchEnd) {
                throw new IllegalArgumentException(
                    "Invalid time range for " + referenceName + ": start (" + stitchStart + ") > end (" + stitchEnd + ")"
                );
            }
        }

        public String getReferenceName() {
            return referenceName;
        }

        public long getStitchStart() {
            return stitchStart;
        }

        public long getStitchEnd() {
            return stitchEnd;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StitchMetadata that = (StitchMetadata) o;
            return stitchStart == that.stitchStart && stitchEnd == that.stitchEnd && Objects.equals(referenceName, that.referenceName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(referenceName, stitchStart, stitchEnd);
        }

        @Override
        public String toString() {
            return "StitchMetadata{" + "ref=" + referenceName + ", start=" + stitchStart + ", end=" + stitchEnd + '}';
        }
    }

    // LinkedHashMap to preserve insertion order for deterministic behavior
    private final LinkedHashMap<String, StitchMetadata> subQueries;
    private final SampleMerger sampleMerger;

    /**
     * Create a StitchStage with the specified sub-query metadata.
     *
     * @param subQueries Map of reference names to their stitch metadata
     */
    public StitchStage(Map<String, StitchMetadata> subQueries) {
        this(subQueries, SampleMerger.DeduplicatePolicy.ANY_WINS);
    }

    /**
     * Create a StitchStage with custom deduplication policy.
     *
     * @param subQueries Map of reference names to their stitch metadata
     * @param deduplicatePolicy Policy for handling duplicate timestamps
     */
    public StitchStage(Map<String, StitchMetadata> subQueries, SampleMerger.DeduplicatePolicy deduplicatePolicy) {
        if (subQueries == null || subQueries.isEmpty()) {
            throw new IllegalArgumentException("Sub-queries map cannot be null or empty");
        }

        // Preserve insertion order for deterministic stitching
        this.subQueries = new LinkedHashMap<>(subQueries);
        this.sampleMerger = new SampleMerger(deduplicatePolicy);

        // Validate that all metadata reference names match their map keys
        for (Map.Entry<String, StitchMetadata> entry : subQueries.entrySet()) {
            if (!entry.getKey().equals(entry.getValue().getReferenceName())) {
                throw new IllegalArgumentException(
                    "Metadata reference name '" + entry.getValue().getReferenceName() + "' does not match map key '" + entry.getKey() + "'"
                );
            }
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<String> getInputReferences() {
        return new ArrayList<>(subQueries.keySet());
    }

    /**
     * Process multiple sub-query results and stitch them into continuous time series.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Group time series from all sub-queries by labels</li>
     *   <li>For each label group, collect samples from all sub-queries</li>
     *   <li>Merge samples in time order using SampleMerger</li>
     *   <li>Create stitched TimeSeries with combined time range</li>
     * </ol>
     *
     * <p><strong>Assumptions:</strong> Samples within each input time series are assumed to be sorted by timestamp.
     * This is a standard invariant for TimeSeries objects returned from aggregations.</p>
     *
     * @param inputs Map of reference names to time series lists
     * @return Stitched time series with continuous data
     */
    @Override
    public List<TimeSeries> process(Map<String, List<TimeSeries>> inputs) {
        if (inputs == null) {
            throw new NullPointerException(getName() + " stage received null inputs");
        }

        // Validate all required references are present
        for (String refName : subQueries.keySet()) {
            if (!inputs.containsKey(refName)) {
                throw new IllegalArgumentException(
                    "Required sub-query reference '" + refName + "' not found in inputs. Available: " + inputs.keySet()
                );
            }
        }

        // Check if all inputs are empty - early return
        boolean allEmpty = inputs.values().stream().allMatch(list -> list == null || list.isEmpty());
        if (allEmpty) {
            return List.of();
        }

        // Group time series by labels across all sub-queries
        Map<Labels, List<TimeSeriesWithMetadata>> groupedByLabels = new HashMap<>();

        for (Map.Entry<String, StitchMetadata> entry : subQueries.entrySet()) {
            String refName = entry.getKey();
            StitchMetadata metadata = entry.getValue();
            List<TimeSeries> timeSeriesList = inputs.get(refName);

            if (timeSeriesList == null || timeSeriesList.isEmpty()) {
                continue; // Skip if no data for this reference
            }

            for (TimeSeries ts : timeSeriesList) {
                groupedByLabels.computeIfAbsent(ts.getLabels(), k -> new ArrayList<>()).add(new TimeSeriesWithMetadata(ts, metadata));
            }
        }

        // Stitch time series for each label group
        List<TimeSeries> result = new ArrayList<>(groupedByLabels.size());
        for (Map.Entry<Labels, List<TimeSeriesWithMetadata>> entry : groupedByLabels.entrySet()) {
            Labels labels = entry.getKey();
            List<TimeSeriesWithMetadata> timeSeriesList = entry.getValue();

            // Sort by stitch start time to ensure correct stitching order
            timeSeriesList.sort(Comparator.comparingLong(tsm -> tsm.metadata.stitchStart));

            // Validate time range continuity (optional - log warning if gaps/overlaps detected)
            validateTimeRangeContinuity(labels, timeSeriesList);

            TimeSeries stitched = stitchTimeSeries(labels, timeSeriesList);
            result.add(stitched);
        }

        return result;
    }

    /**
     * Stitch multiple time series with the same labels into a single continuous time series.
     *
     * @param labels Common labels for all time series
     * @param timeSeriesList List of time series to stitch (sorted by time range)
     * @return Stitched time series
     */
    private TimeSeries stitchTimeSeries(Labels labels, List<TimeSeriesWithMetadata> timeSeriesList) {
        if (timeSeriesList.isEmpty()) {
            throw new IllegalArgumentException("Cannot stitch empty time series list");
        }

        if (timeSeriesList.size() == 1) {
            // Single time series, filter to stitch range
            TimeSeriesWithMetadata tsm = timeSeriesList.get(0);
            SampleList filteredSamples = filterSamplesToStitchRange(tsm.timeSeries.getSamples(), tsm.metadata);
            return new TimeSeries(
                filteredSamples,
                labels,
                tsm.timeSeries.getMinTimestamp(),
                tsm.timeSeries.getMaxTimestamp(),
                tsm.timeSeries.getStep(),
                tsm.timeSeries.getAlias()
            );
        }

        // Collect all samples from all time series, filtered to their stitch ranges
        SampleList mergedSamples = null;
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;
        long step = timeSeriesList.get(0).timeSeries.getStep(); // Use step from first series
        String alias = timeSeriesList.get(0).timeSeries.getAlias(); // Use alias from first series

        for (TimeSeriesWithMetadata tsm : timeSeriesList) {
            TimeSeries ts = tsm.timeSeries;

            // Filter samples to only those within the stitch range
            SampleList filteredSamples = filterSamplesToStitchRange(ts.getSamples(), tsm.metadata);

            if (filteredSamples.size() == 0) {
                continue; // Skip if no samples in range
            }

            // Update time range bounds
            minTimestamp = Math.min(minTimestamp, ts.getMinTimestamp());
            maxTimestamp = Math.max(maxTimestamp, ts.getMaxTimestamp());

            // Merge samples
            if (mergedSamples == null) {
                mergedSamples = filteredSamples;
            } else {
                // Assume samples within each time series are sorted
                // Use sorted merge for efficiency
                mergedSamples = sampleMerger.merge(mergedSamples, filteredSamples, true);
            }
        }

        if (mergedSamples == null || mergedSamples.size() == 0) {
            // No samples after filtering - return empty time series with empty sample list
            mergedSamples = SampleList.fromList(List.of());
        }

        return new TimeSeries(mergedSamples, labels, minTimestamp, maxTimestamp, step, alias);
    }

    /**
     * Filter samples to only include those within the stitch range [stitchStart, stitchEnd).
     * The stitch range is inclusive of stitchStart and exclusive of stitchEnd.
     *
     * @param samples The samples to filter
     * @param metadata The stitch metadata containing the time range
     * @return Filtered sample list
     */
    private SampleList filterSamplesToStitchRange(SampleList samples, StitchMetadata metadata) {
        if (samples.size() == 0) {
            return samples;
        }

        List<Sample> filteredSamples = new ArrayList<>();

        for (int i = 0; i < samples.size(); i++) {
            long timestamp = samples.getTimestamp(i);
            // Include samples where: stitchStart <= timestamp < stitchEnd
            if (timestamp >= metadata.stitchStart && timestamp < metadata.stitchEnd) {
                filteredSamples.add(new FloatSample(timestamp, samples.getValue(i)));
            }
        }

        return SampleList.fromList(filteredSamples);
    }

    /**
     * Validate that time ranges are continuous (no gaps or overlaps).
     * Logs warnings if gaps or overlaps are detected but does not fail the operation.
     *
     * @param labels Labels for this time series (for logging purposes)
     * @param timeSeriesList List of time series with metadata (sorted by stitch start time)
     */
    private void validateTimeRangeContinuity(Labels labels, List<TimeSeriesWithMetadata> timeSeriesList) {
        if (timeSeriesList.size() <= 1) {
            return; // No validation needed for single series
        }

        for (int i = 0; i < timeSeriesList.size() - 1; i++) {
            TimeSeriesWithMetadata current = timeSeriesList.get(i);
            TimeSeriesWithMetadata next = timeSeriesList.get(i + 1);

            long currentEnd = current.metadata.stitchEnd;
            long nextStart = next.metadata.stitchStart;

            if (currentEnd < nextStart) {
                // Gap detected
                logger.warn(
                    "Gap detected in stitch time ranges for series {}: {} ends at {}, {} starts at {} (gap: {}ms)",
                    labels,
                    current.metadata.referenceName,
                    currentEnd,
                    next.metadata.referenceName,
                    nextStart,
                    nextStart - currentEnd
                );
            } else if (currentEnd > nextStart) {
                // Overlap detected
                logger.warn(
                    "Overlap detected in stitch time ranges for series {}: {} ends at {}, {} starts at {} (overlap: {}ms)",
                    labels,
                    current.metadata.referenceName,
                    currentEnd,
                    next.metadata.referenceName,
                    nextStart,
                    currentEnd - nextStart
                );
            }
        }
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(SUB_QUERIES_PARAM_KEY);
        for (Map.Entry<String, StitchMetadata> entry : subQueries.entrySet()) {
            builder.startObject(entry.getKey());
            builder.field("stitch_start", entry.getValue().stitchStart);
            builder.field("stitch_end", entry.getValue().stitchEnd);
            builder.endObject();
        }
        builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(subQueries.size());
        for (Map.Entry<String, StitchMetadata> entry : subQueries.entrySet()) {
            out.writeString(entry.getKey());
            out.writeLong(entry.getValue().stitchStart);
            out.writeLong(entry.getValue().stitchEnd);
        }
    }

    /**
     * Create a StitchStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new StitchStage instance
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public static StitchStage readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, StitchMetadata> subQueries = new LinkedHashMap<>(size);

        for (int i = 0; i < size; i++) {
            String refName = in.readString();
            long stitchStart = in.readLong();
            long stitchEnd = in.readLong();
            subQueries.put(refName, new StitchMetadata(refName, stitchStart, stitchEnd));
        }

        return new StitchStage(subQueries);
    }

    /**
     * Creates a new instance of StitchStage from arguments map.
     *
     * @param args a map containing sub_queries with metadata
     * @return a new StitchStage instance
     */
    @SuppressWarnings("unchecked")
    public static StitchStage fromArgs(Map<String, Object> args) {
        Object subQueriesObj = args.get(SUB_QUERIES_PARAM_KEY);
        if (!(subQueriesObj instanceof Map)) {
            throw new IllegalArgumentException("Expected 'sub_queries' to be a Map, got: " + subQueriesObj);
        }

        Map<String, Object> subQueriesMap = (Map<String, Object>) subQueriesObj;
        Map<String, StitchMetadata> subQueries = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : subQueriesMap.entrySet()) {
            String refName = entry.getKey();
            Object metadataObj = entry.getValue();

            if (!(metadataObj instanceof Map)) {
                throw new IllegalArgumentException("Expected metadata for '" + refName + "' to be a Map, got: " + metadataObj);
            }

            Map<String, Object> metadataMap = (Map<String, Object>) metadataObj;
            Object stitchStartObj = metadataMap.get("stitch_start");
            Object stitchEndObj = metadataMap.get("stitch_end");

            if (!(stitchStartObj instanceof Number) || !(stitchEndObj instanceof Number)) {
                throw new IllegalArgumentException("stitch_start and stitch_end must be numbers for " + refName);
            }

            long stitchStart = ((Number) stitchStartObj).longValue();
            long stitchEnd = ((Number) stitchEndObj).longValue();

            subQueries.put(refName, new StitchMetadata(refName, stitchStart, stitchEnd));
        }

        return new StitchStage(subQueries);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StitchStage that = (StitchStage) o;
        return Objects.equals(subQueries, that.subQueries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subQueries);
    }

    /**
     * Helper class to associate a TimeSeries with its stitch metadata.
     */
    private static class TimeSeriesWithMetadata {
        final TimeSeries timeSeries;
        final StitchMetadata metadata;

        TimeSeriesWithMetadata(TimeSeries timeSeries, StitchMetadata metadata) {
            this.timeSeries = timeSeries;
            this.metadata = metadata;
        }
    }
}
