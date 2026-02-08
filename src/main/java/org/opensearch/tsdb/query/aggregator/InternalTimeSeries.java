/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.query.utils.SampleMerger;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Internal aggregation result for time series pipeline aggregators.
 *
 * <p>This class represents the result of time series pipeline aggregations, supporting
 * both decoded samples and compressed chunks based on the encoding mode. It implements
 * the {@link TimeSeriesProvider} interface to provide access to the underlying time series data.</p>
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li><strong>Time Series Storage:</strong> Maintains a list of time series with their
 *       associated samples, labels, and metadata</li>
 *   <li><strong>Reduce Stage Support:</strong> Supports optional reduce stages for
 *       final aggregation operations</li>
 *   <li><strong>Label-based Merging:</strong> Uses {@link SampleMerger} for
 *       intelligent merging of time series with matching labels</li>
 *   <li><strong>Serialization:</strong> Supports streaming serialization/deserialization
 *       for distributed processing and records serialized byte size for network metrics when possible</li>
 *   <li><strong>Encoding Modes:</strong> Supports two encoding modes for network transmission:
 *     <ul>
 *       <li><strong>NONE:</strong> Decoded samples sent over the wire (used when pipeline stages
 *           need to be applied on data nodes)</li>
 *       <li><strong>XOR:</strong> Compressed chunks sent over the wire (used when data nodes have
 *           no pipeline stages to process, minimizing network transfer and data node CPU usage)</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <p>Data nodes choose the encoding based on whether pipeline stages need to be applied locally.
 * The coordinator decodes compressed data during the reduce phase when needed. When merging
 * segment results on a data node (e.g. CSS), payload can be kept compressed and only decoded
 * on the coordinator.</p>
 */
public class InternalTimeSeries extends InternalAggregation implements TimeSeriesProvider {

    /**
     * Format marker for wire serialization.
     * Using -1 because old format (VInt timeSeriesCount) can never be negative.
     * This allows self-describing format detection: if first VInt == -1, new format (read encoding
     * byte next); if first VInt >= 0, old format (value is timeSeriesCount).
     * Future versions (e.g. -2) may add further format changes.
     */
    private static final int WIRE_FORMAT_VERSION_1 = -1;

    /**
     * Encoding format for time series data transmission.
     */
    public enum Encoding {
        NONE((byte) 0),  // Decoded samples
        XOR((byte) 1);   // XOR-compressed chunks

        private final byte id;

        Encoding(byte id) {
            this.id = id;
        }

        public byte getId() {
            return id;
        }

        public static Encoding fromId(byte id) {
            for (Encoding encoding : values()) {
                if (encoding.id == id) {
                    return encoding;
                }
            }
            throw new IllegalArgumentException("Unknown encoding ID: " + id);
        }
    }

    /**
     * Sealed interface representing encoded time series data.
     * Each encoding type has its own implementation for encoding-specific logic.
     */
    private sealed interface EncodedData permits DecodedData, CompressedData {
        Encoding getEncoding();

        List<TimeSeries> decode();

        /** Returns the raw compressed list when encoding is XOR; empty when NONE. Used to merge without decoding on data node (CSS). */
        List<CompressedTimeSeries> getCompressedTimeSeries();

        void writeTo(StreamOutput out) throws IOException;

        boolean dataEquals(EncodedData other);

        int dataHashCode();

        UnaryPipelineStage getReduceStage();
    }

    /**
     * Decoded time series data (NONE encoding).
     */
    private static final class DecodedData implements EncodedData {
        private final List<TimeSeries> timeSeriesList;
        private final UnaryPipelineStage reduceStage;

        DecodedData(List<TimeSeries> timeSeriesList, UnaryPipelineStage reduceStage) {
            this.timeSeriesList = timeSeriesList;
            this.reduceStage = reduceStage;
        }

        @Override
        public Encoding getEncoding() {
            return Encoding.NONE;
        }

        @Override
        public List<TimeSeries> decode() {
            return timeSeriesList;
        }

        @Override
        public List<CompressedTimeSeries> getCompressedTimeSeries() {
            return List.of();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(WIRE_FORMAT_VERSION_1);
            out.writeByte(Encoding.NONE.getId());
            List<TimeSeries> seriesList = timeSeriesList != null ? timeSeriesList : List.of();
            out.writeVInt(seriesList.size());
            for (TimeSeries series : seriesList) {
                out.writeInt(0);  // hash - placeholder for now
                SampleList samples = series.getSamples();
                out.writeVInt(samples.size());
                for (Sample sample : samples) {
                    sample.writeTo(out);
                }
                Map<String, String> labelsMap = series.getLabels() != null ? series.getLabels().toMapView() : new HashMap<>();
                out.writeMap(labelsMap, StreamOutput::writeString, StreamOutput::writeString);
                out.writeOptionalString(series.getAlias());
                out.writeLong(series.getMinTimestamp());
                out.writeLong(series.getMaxTimestamp());
                out.writeLong(series.getStep());
            }
            if (reduceStage != null) {
                out.writeBoolean(true);
                out.writeString(reduceStage.getName());
                reduceStage.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public UnaryPipelineStage getReduceStage() {
            return reduceStage;
        }

        @Override
        public boolean dataEquals(EncodedData other) {
            if (!(other instanceof DecodedData that)) return false;
            return timeSeriesListEquals(timeSeriesList, that.timeSeriesList)
                && Objects.equals(
                    reduceStage != null ? reduceStage.getName() : null,
                    that.reduceStage != null ? that.reduceStage.getName() : null
                );
        }

        @Override
        public int dataHashCode() {
            return Objects.hash(timeSeriesListHashCode(timeSeriesList), reduceStage != null ? reduceStage.getName() : null);
        }

        static DecodedData readFrom(StreamInput in) throws IOException {
            int timeSeriesCount = in.readVInt();
            List<TimeSeries> timeSeriesList = new ArrayList<>(timeSeriesCount);
            for (int i = 0; i < timeSeriesCount; i++) {
                timeSeriesList.add(readTimeSeries(in));
            }
            boolean hasReduceStage = in.readBoolean();
            UnaryPipelineStage reduceStage = null;
            if (hasReduceStage) {
                String stageName = in.readString();
                reduceStage = (UnaryPipelineStage) PipelineStageFactory.readFrom(in, stageName);
            }
            return new DecodedData(timeSeriesList, reduceStage);
        }

        /**
         * Read DecodedData from old format (pre-compressed mode) where no marker or encoding byte was written.
         *
         * @param in the stream input
         * @param timeSeriesCount the already-read time series count (was read during format detection)
         */
        static DecodedData readFromLegacy(StreamInput in, int timeSeriesCount) throws IOException {
            List<TimeSeries> timeSeriesList = new ArrayList<>(timeSeriesCount);
            for (int i = 0; i < timeSeriesCount; i++) {
                timeSeriesList.add(readTimeSeries(in));
            }
            boolean hasReduceStage = in.readBoolean();
            UnaryPipelineStage reduceStage = null;
            if (hasReduceStage) {
                String stageName = in.readString();
                reduceStage = (UnaryPipelineStage) PipelineStageFactory.readFrom(in, stageName);
            }
            return new DecodedData(timeSeriesList, reduceStage);
        }
    }

    /**
     * XOR-compressed time series data (XOR encoding).
     */
    private static final class CompressedData implements EncodedData {
        private final List<CompressedTimeSeries> compressedTimeSeries;

        CompressedData(List<CompressedTimeSeries> compressedTimeSeries) {
            this.compressedTimeSeries = compressedTimeSeries != null ? compressedTimeSeries : List.of();
        }

        @Override
        public Encoding getEncoding() {
            return Encoding.XOR;
        }

        @Override
        public List<TimeSeries> decode() {
            return decodeCompressedTimeSeries(compressedTimeSeries);
        }

        @Override
        public List<CompressedTimeSeries> getCompressedTimeSeries() {
            return compressedTimeSeries;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(WIRE_FORMAT_VERSION_1);
            out.writeByte(Encoding.XOR.getId());
            out.writeVInt(compressedTimeSeries.size());
            for (CompressedTimeSeries series : compressedTimeSeries) {
                series.writeTo(out);
            }
        }

        @Override
        public boolean dataEquals(EncodedData other) {
            if (!(other instanceof CompressedData that)) return false;
            return Objects.equals(compressedTimeSeries, that.compressedTimeSeries);
        }

        @Override
        public int dataHashCode() {
            return Objects.hash(compressedTimeSeries);
        }

        @Override
        public UnaryPipelineStage getReduceStage() {
            return null;
        }

        /**
         * Decodes a list of compressed time series into decoded time series.
         * Groups by labels, decodes each group's chunks, merges samples, then aligns and deduplicates.
         */
        private static List<TimeSeries> decodeCompressedTimeSeries(List<CompressedTimeSeries> compressedList) {
            Map<Labels, List<CompressedTimeSeries>> seriesByLabels = new HashMap<>();
            for (CompressedTimeSeries compressedSeries : compressedList) {
                seriesByLabels.computeIfAbsent(compressedSeries.getLabels(), k -> new ArrayList<>()).add(compressedSeries);
            }
            List<TimeSeries> decodedTimeSeries = new ArrayList<>(seriesByLabels.size());
            for (Map.Entry<Labels, List<CompressedTimeSeries>> entry : seriesByLabels.entrySet()) {
                Labels labels = entry.getKey();
                List<CompressedTimeSeries> compressedGroup = entry.getValue();
                long overallMinTimestamp = Long.MAX_VALUE;
                long overallMaxTimestamp = Long.MIN_VALUE;
                for (CompressedTimeSeries series : compressedGroup) {
                    overallMinTimestamp = Math.min(overallMinTimestamp, series.getMinTimestamp());
                    overallMaxTimestamp = Math.max(overallMaxTimestamp, series.getMaxTimestamp());
                }
                long overallStep = compressedGroup.get(0).getStep();
                String alias = compressedGroup.get(0).getAlias();
                List<List<Sample>> allSamplesToMerge = new ArrayList<>(compressedGroup.size());
                for (CompressedTimeSeries series : compressedGroup) {
                    try {
                        // maxTimestamp+1 because decodeAllSamples uses exclusive upper bound
                        allSamplesToMerge.add(series.decodeAllSamples(overallMinTimestamp, overallMaxTimestamp + 1));
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to decode compressed chunks for series: " + labels, e);
                    }
                }
                List<Sample> mergedSamples;
                if (allSamplesToMerge.isEmpty()) {
                    mergedSamples = List.of();
                } else {
                    mergedSamples = allSamplesToMerge.get(0);
                    for (int i = 1; i < allSamplesToMerge.size(); i++) {
                        SampleList merged = MERGE_HELPER.merge(
                            SampleList.fromList(mergedSamples),
                            SampleList.fromList(allSamplesToMerge.get(i)),
                            true
                        );
                        mergedSamples = merged.toList();
                    }
                }
                List<Sample> alignedSamples = SampleMerger.alignAndDeduplicate(mergedSamples, overallMinTimestamp, overallStep);
                decodedTimeSeries.add(new TimeSeries(alignedSamples, labels, overallMinTimestamp, overallMaxTimestamp, overallStep, alias));
            }
            return decodedTimeSeries;
        }

        static CompressedData readFrom(StreamInput in) throws IOException {
            int count = in.readVInt();
            List<CompressedTimeSeries> compressedTimeSeries = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                compressedTimeSeries.add(new CompressedTimeSeries(in));
            }
            return new CompressedData(compressedTimeSeries);
        }
    }

    private final EncodedData data;
    private static final SampleMerger MERGE_HELPER = new SampleMerger(SampleMerger.DeduplicatePolicy.ANY_WINS);

    /**
     * Creates a new InternalTimeSeries with decoded samples (encoding = NONE).
     *
     * @param name the name of the aggregation
     * @param timeSeriesList the list of decoded time series data
     * @param metadata the aggregation metadata
     */
    public InternalTimeSeries(String name, List<TimeSeries> timeSeriesList, Map<String, Object> metadata) {
        this(name, timeSeriesList, metadata, null);
    }

    /**
     * Creates a new InternalTimeSeries with decoded samples and optional reduce stage.
     *
     * @param name the name of the aggregation
     * @param timeSeriesList the list of decoded time series data
     * @param metadata the aggregation metadata
     * @param reduceStage the optional reduce stage for final aggregation operations
     */
    public InternalTimeSeries(String name, List<TimeSeries> timeSeriesList, Map<String, Object> metadata, UnaryPipelineStage reduceStage) {
        super(name, metadata);
        this.data = new DecodedData(timeSeriesList, reduceStage);
    }

    /** Private constructor for creating InternalTimeSeries with specific encoded data. */
    private InternalTimeSeries(String name, EncodedData data, Map<String, Object> metadata) {
        super(name, metadata);
        this.data = data;
    }

    /**
     * Creates a new InternalTimeSeries with compressed chunks (encoding = XOR).
     *
     * @param name the name of the aggregation
     * @param compressedTimeSeries the list of compressed time series data
     * @param metadata the aggregation metadata
     * @return a new InternalTimeSeries with XOR encoding
     */
    public static InternalTimeSeries compressed(
        String name,
        List<CompressedTimeSeries> compressedTimeSeries,
        Map<String, Object> metadata
    ) {
        return new InternalTimeSeries(name, new CompressedData(compressedTimeSeries), metadata);
    }

    /**
     * Reads an InternalTimeSeries from a stream for deserialization.
     * Handles backward compatibility with old builds using format marker detection.
     *
     * <p>Detection: if first VInt == WIRE_FORMAT_VERSION_1 (-1), new format (read encoding byte, then data);
     * if first VInt >= 0, old format (value is timeSeriesCount, read data directly). This is self-describing
     * so it works across coordinator and data clusters without version negotiation.</p>
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public InternalTimeSeries(StreamInput in) throws IOException {
        super(in);
        int firstValue = in.readVInt();
        if (firstValue == WIRE_FORMAT_VERSION_1) {
            Encoding encoding = Encoding.fromId(in.readByte());
            this.data = switch (encoding) {
                case NONE -> DecodedData.readFrom(in);
                case XOR -> CompressedData.readFrom(in);
            };
        } else if (firstValue >= 0) {
            this.data = DecodedData.readFromLegacy(in, firstValue);
        } else {
            throw new IOException("Invalid format marker or timeSeriesCount: " + firstValue);
        }
    }

    /**
     * Serializes this aggregation and records serialized byte size for network metrics when the
     * stream supports {@link StreamOutput#position()}. Metrics must not affect serialization.
     */
    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        long startPos = getStreamPosition(out);
        data.writeTo(out);
        long endPos = getStreamPosition(out);
        if (startPos >= 0 && endPos >= 0 && endPos > startPos) {
            long serializedBytes = endPos - startPos;
            try {
                if (data.getEncoding() == Encoding.XOR) {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.compressedBytesTotal, serializedBytes);
                } else {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.decodedBytesTotal, serializedBytes);
                }
            } catch (Exception ignored) {
                // Metrics must not break serialization
            }
        }
    }

    /**
     * Returns the current write position when the stream supports it, else -1.
     * Used to measure serialized payload size; transport uses {@code BytesStreamOutput} which overrides {@link StreamOutput#position()}.
     */
    private static long getStreamPosition(StreamOutput out) {
        try {
            return out.position();
        } catch (UnsupportedOperationException | IOException e) {
            return -1;
        }
    }

    /**
     * Returns the writeable name used for stream serialization.
     *
     * @return the writeable name "time_series"
     */
    @Override
    public String getWriteableName() {
        return "time_series";
    }

    /**
     * Reduces multiple InternalTimeSeries aggregations into a single result.
     *
     * <p>Handles: (1) When a reduce stage is present, decodes XOR if needed and delegates to the stage;
     * (2) When merging segment results on a data node (partial reduce) and all aggs are XOR, merges
     * compressed payload without decoding; (3) Otherwise (final reduce or any NONE data), decodes and
     * merges time series by labels using {@link SampleMerger}.</p>
     *
     * @param aggregations the list of aggregations to reduce
     * @param reduceContext the context for the reduce operation
     * @return the reduced aggregation result
     * @throws IllegalArgumentException if any aggregation is not an InternalTimeSeries
     */
    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        UnaryPipelineStage reduceStage = getReduceStage();
        if (reduceStage != null) {
            List<TimeSeriesProvider> timeSeriesProviders = new ArrayList<>(aggregations.size());
            for (InternalAggregation agg : aggregations) {
                if (!(agg instanceof InternalTimeSeries)) {
                    throw new IllegalArgumentException("Expected InternalTimeSeries but got: " + agg.getClass());
                }
                InternalTimeSeries its = (InternalTimeSeries) agg;
                switch (its.getEncoding()) {
                    case XOR:
                        List<TimeSeries> decoded = its.getTimeSeries();
                        timeSeriesProviders.add(new InternalTimeSeries(its.name, decoded, its.metadata, null));
                        break;
                    case NONE:
                        timeSeriesProviders.add(its);
                        break;
                }
            }
            return reduceStage.reduce(timeSeriesProviders, reduceContext.isFinalReduce());
        }

        // When merging segment results on a data node (CSS), keep payload compressed so we only decode on coordinator.
        // Only use compressed merge when all aggs are XOR; otherwise (e.g. NONE from non-compressed path) decode and merge.
        if (!reduceContext.isFinalReduce() && aggregations.stream().allMatch(a -> ((InternalTimeSeries) a).getEncoding() == Encoding.XOR)) {
            return mergeCompressedWithoutDecoding(aggregations);
        }

        // Coordinator final reduce, or partial reduce with any decoded (NONE) data: decode and merge by labels
        Map<Labels, TimeSeries> mergedSeriesByLabels = new HashMap<>();
        for (InternalAggregation agg : aggregations) {
            if (!(agg instanceof InternalTimeSeries)) {
                throw new IllegalArgumentException("Expected InternalTimeSeries but got: " + agg.getClass());
            }
            InternalTimeSeries its = (InternalTimeSeries) agg;
            List<TimeSeries> timeSeriesList = its.getTimeSeries();
            if (timeSeriesList == null) continue;

            for (TimeSeries series : timeSeriesList) {
                Labels seriesLabels = series.getLabels();
                TimeSeries existingSeries = mergedSeriesByLabels.get(seriesLabels);
                if (existingSeries != null) {
                    // Merge samples from same time series across segments
                    SampleList mergedSamples = MERGE_HELPER.merge(existingSeries.getSamples(), series.getSamples(), true);
                    TimeSeries mergedSeries = new TimeSeries(
                        mergedSamples,
                        existingSeries.getLabels(),
                        existingSeries.getMinTimestamp(),
                        existingSeries.getMaxTimestamp(),
                        existingSeries.getStep(),
                        existingSeries.getAlias()
                    );
                    mergedSeriesByLabels.put(seriesLabels, mergedSeries);
                } else {
                    mergedSeriesByLabels.put(seriesLabels, series);
                }
            }
        }
        return new InternalTimeSeries(name, new ArrayList<>(mergedSeriesByLabels.values()), metadata, null);
    }

    /**
     * Merges segment-level compressed results without decoding (data node / CSS merge).
     * Groups by labels, concatenates chunks per group, and keeps XOR encoding so decompression
     * only happens on the coordinator.
     */
    private InternalAggregation mergeCompressedWithoutDecoding(List<InternalAggregation> aggregations) {
        Map<Labels, List<CompressedTimeSeries>> byLabels = new HashMap<>();
        for (InternalAggregation agg : aggregations) {
            InternalTimeSeries its = (InternalTimeSeries) agg;
            List<CompressedTimeSeries> list = its.getCompressedTimeSeries();
            if (list == null || list.isEmpty()) continue;
            for (CompressedTimeSeries cts : list) {
                byLabels.computeIfAbsent(cts.getLabels(), k -> new ArrayList<>()).add(cts);
            }
        }
        List<CompressedTimeSeries> merged = new ArrayList<>(byLabels.size());
        for (Map.Entry<Labels, List<CompressedTimeSeries>> e : byLabels.entrySet()) {
            List<CompressedTimeSeries> group = e.getValue();
            if (group.isEmpty()) continue;
            if (group.size() == 1) {
                merged.add(group.get(0));
                continue;
            }
            long minTs = Long.MAX_VALUE;
            long maxTs = Long.MIN_VALUE;
            List<CompressedChunk> allChunks = new ArrayList<>();
            for (CompressedTimeSeries cts : group) {
                minTs = Math.min(minTs, cts.getMinTimestamp());
                maxTs = Math.max(maxTs, cts.getMaxTimestamp());
                allChunks.addAll(cts.getChunks());
            }
            CompressedTimeSeries first = group.get(0);
            merged.add(new CompressedTimeSeries(allChunks, first.getLabels(), minTs, maxTs, first.getStep(), first.getAlias()));
        }
        return InternalTimeSeries.compressed(name, merged, metadata);
    }

    /**
     * Returns raw compressed time series when encoding is XOR; empty list when NONE.
     * For internal reduce only (e.g. mergeCompressedWithoutDecoding).
     */
    List<CompressedTimeSeries> getCompressedTimeSeries() {
        return data.getCompressedTimeSeries();
    }

    /**
     * Returns a property value for this aggregation.
     * Supports empty path (returns this) and "timeSeries" (returns decoded list).
     */
    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) return this;
        if (path.size() == 1 && "timeSeries".equals(path.get(0))) return getTimeSeries();
        throw new IllegalArgumentException("Unknown property [" + path.get(0) + "] for " + getClass().getSimpleName() + " [" + name + "]");
    }

    /**
     * Returns the list of time series contained in this aggregation result.
     * Decodes compressed data if encoding is XOR.
     *
     * @return the list of time series data
     */
    @Override
    public List<TimeSeries> getTimeSeries() {
        return data.decode();
    }

    /**
     * Gets the reduce stage associated with this aggregation result.
     *
     * @return the reduce stage, or null if there is no reduce stage
     */
    public UnaryPipelineStage getReduceStage() {
        return data.getReduceStage();
    }

    /**
     * Creates a new TimeSeriesProvider with the given time series data.
     * Always returns NONE encoding (decoded) after reduction.
     *
     * @param timeSeriesList the new time series data
     * @return a new InternalTimeSeries instance with the provided data
     */
    @Override
    public TimeSeriesProvider createReduced(List<TimeSeries> timeSeriesList) {
        return new InternalTimeSeries(name, timeSeriesList, metadata, getReduceStage());
    }

    /**
     * Renders the time series data as XContent (JSON).
     * Decodes compressed data if needed before rendering.
     */
    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("timeSeries");
        List<TimeSeries> timeSeriesList = getTimeSeries();
        if (timeSeriesList != null) {
            for (TimeSeries series : timeSeriesList) {
                builder.startObject();
                builder.field("hash", 0);  // placeholder for now
                if (series.getAlias() != null) builder.field("alias", series.getAlias());
                builder.field("minTimestamp", series.getMinTimestamp());
                builder.field("maxTimestamp", series.getMaxTimestamp());
                builder.field("step", series.getStep());
                builder.startArray("samples");
                for (Sample sample : series.getSamples()) {
                    builder.startObject();
                    builder.field("timestamp", sample.getTimestamp());
                    builder.field("value", sample.getValue());
                    builder.endObject();
                }
                builder.endArray();
                if (series.getLabels() != null && !series.getLabels().isEmpty()) {
                    builder.field("labels", series.getLabels().toMapView());
                }
                builder.endObject();
            }
        }
        builder.endArray();
        return builder;
    }

    /**
     * Indicates whether this aggregation must be reduced even when there is only one shard.
     */
    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    /**
     * Reads a TimeSeries object from a stream input during deserialization.
     *
     * <p>Deserializes: hash (placeholder), sample count and samples, labels, optional alias,
     * and time series metadata (min/max timestamp, step).</p>
     *
     * @param in the stream input to read from
     * @return the deserialized TimeSeries object
     * @throws IOException if an I/O error occurs during reading
     */
    private static TimeSeries readTimeSeries(StreamInput in) throws IOException {
        int hash = in.readInt();
        int sampleCount = in.readVInt();
        List<Sample> samples = new ArrayList<>(sampleCount);
        for (int i = 0; i < sampleCount; i++) {
            samples.add(Sample.readFrom(in));
        }
        Map<String, String> labelsMap = in.readMap(StreamInput::readString, StreamInput::readString);
        Labels labels = labelsMap.isEmpty() ? ByteLabels.emptyLabels() : ByteLabels.fromMap(labelsMap);
        String alias = in.readOptionalString();
        long minTimestamp = in.readLong();
        long maxTimestamp = in.readLong();
        long step = in.readLong();
        return new TimeSeries(samples, labels, minTimestamp, maxTimestamp, step, alias);
    }

    /**
     * Returns the encoding mode of this aggregation result.
     *
     * @return the encoding mode (NONE or XOR)
     */
    public Encoding getEncoding() {
        return data.getEncoding();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalTimeSeries that = (InternalTimeSeries) o;
        return Objects.equals(getName(), that.getName())
            && Objects.equals(getMetadata(), that.getMetadata())
            && data.getEncoding() == that.data.getEncoding()
            && data.dataEquals(that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getMetadata(), data.getEncoding(), data.dataHashCode());
    }

    /** Compares two time series lists for equality. */
    private static boolean timeSeriesListEquals(List<TimeSeries> list1, List<TimeSeries> list2) {
        if (list1 == list2) return true;
        if (list1 == null || list2 == null) return false;
        if (list1.size() != list2.size()) return false;
        for (int i = 0; i < list1.size(); i++) {
            TimeSeries ts1 = list1.get(i);
            TimeSeries ts2 = list2.get(i);
            if (!Objects.equals(ts1.getAlias(), ts2.getAlias())) return false;
            if (ts1.getMinTimestamp() != ts2.getMinTimestamp()) return false;
            if (ts1.getMaxTimestamp() != ts2.getMaxTimestamp()) return false;
            if (ts1.getStep() != ts2.getStep()) return false;
            if (!ts1.getLabels().toMapView().equals(ts2.getLabels().toMapView())) return false;
            if (ts1.getSamples().size() != ts2.getSamples().size()) return false;
        }
        return true;
    }

    /** Computes hash code for a time series list. */
    private static int timeSeriesListHashCode(List<TimeSeries> list) {
        if (list == null) return 0;
        int result = 1;
        for (TimeSeries ts : list) {
            result = 31 * result + (ts == null
                ? 0
                : Objects.hash(ts.getAlias(), ts.getMinTimestamp(), ts.getMaxTimestamp(), ts.getStep(), ts.getSamples().size()));
        }
        return result;
    }
}
