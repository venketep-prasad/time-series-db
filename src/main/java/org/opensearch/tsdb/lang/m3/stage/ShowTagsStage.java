/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A pipeline stage that formats series aliases from tag key-value pairs.
 *
 * <p>Behavior:
 * - If no tags specified: shows all available tags (sorted alphabetically by key)
 * - If tags specified: shows only those tags in the order provided
 * - showKeys=true format: "tag1:value1 tag2:value2"
 * - showKeys=false format: "value1 value2"
 * - Missing tags are skipped (only existing tags are shown)
 */
@PipelineStageAnnotation(name = ShowTagsStage.NAME)
public class ShowTagsStage implements UnaryPipelineStage {
    /** The name identifier for this pipeline stage. */
    public static final String NAME = "show_tags";

    private static final String SHOW_KEYS_ARG = "show_keys";
    private static final String TAGS_ARG = "tags";

    private final boolean showKeys;
    private final List<String> tags;

    /**
     * Creates a new ShowTagsStage with the specified parameters.
     *
     * @param showKeys whether to show tag keys in the output
     * @param tags the list of tag names to display (empty list means show all tags)
     */
    public ShowTagsStage(boolean showKeys, List<String> tags) {
        this.showKeys = showKeys;
        this.tags = new ArrayList<>(tags);
    }

    /**
     * Creates a new ShowTagsStage by reading from a stream input.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs while reading
     */
    public ShowTagsStage(final StreamInput in) throws IOException {
        this.showKeys = in.readBoolean();
        this.tags = in.readStringList();
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }

        List<TimeSeries> result = new ArrayList<>(input.size());
        for (TimeSeries ts : input) {
            String alias = buildTagsString(ts.getLabels());
            // Create new TimeSeries with updated alias
            TimeSeries updated = new TimeSeries(
                ts.getSamples(),
                ts.getLabels(),
                ts.getMinTimestamp(),
                ts.getMaxTimestamp(),
                ts.getStep(),
                alias
            );
            result.add(updated);
        }
        return result;
    }

    /**
     * Builds the formatted tags string from labels.
     * Implements the Go TagsToString logic.
     *
     * @param labels The labels containing tag key-value pairs
     * @return The formatted tags string
     */
    private String buildTagsString(Labels labels) {
        if (labels == null) {
            return "";
        }

        Map<String, String> labelsMap = labels.toMapView();
        if (labelsMap.isEmpty()) {
            return "";
        }

        // Determine which keys to include
        List<String> keysToInclude = new ArrayList<>();

        if (tags.isEmpty()) {
            // Show all tags, sorted alphabetically
            keysToInclude.addAll(labelsMap.keySet());
            keysToInclude.sort(String::compareTo);
        } else {
            // Show only specified tags in the order provided
            // Filter out tags that don't exist in the series
            for (String tagName : tags) {
                if (labelsMap.containsKey(tagName)) {
                    keysToInclude.add(tagName);
                }
            }
        }

        // Build the output string
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < keysToInclude.size(); i++) {
            String key = keysToInclude.get(i);
            String value = labelsMap.get(key);

            if (value != null) {
                if (showKeys) {
                    result.append(key).append(':');
                }
                result.append(value);

                // Add space separator between tags (but not after the last one)
                if (i < keysToInclude.size() - 1) {
                    result.append(' ');
                }
            }
        }

        return result.toString();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(SHOW_KEYS_ARG, showKeys);
        builder.field(TAGS_ARG, tags);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(showKeys);
        out.writeStringCollection(tags);
    }

    @Override
    public boolean supportConcurrentSegmentSearch() {
        // This stage is safe for concurrent execution at the shard level
        // It only modifies aliases based on label data, no shared state
        return true;
    }

    /**
     * Create a ShowTagsStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new ShowTagsStage instance
     * @throws IOException if an I/O error occurs while reading
     */
    public static ShowTagsStage readFrom(StreamInput in) throws IOException {
        boolean showKeys = in.readBoolean();
        List<String> tags = in.readStringList();
        return new ShowTagsStage(showKeys, tags);
    }

    /**
     * Creates a new instance of ShowTagsStage using the provided arguments.
     *
     * @param args a map containing the arguments required to construct a ShowTagsStage instance.
     *             Expected keys:
     *             - "show_keys" (Boolean): whether to show tag keys
     *             - "tags" (List&lt;String&gt;): list of tag names to display
     * @return a new ShowTagsStage instance initialized with the provided parameters
     */
    public static ShowTagsStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }

        // showKeys is required in args map
        if (!args.containsKey(SHOW_KEYS_ARG)) {
            throw new IllegalArgumentException("ShowTags stage requires '" + SHOW_KEYS_ARG + "' argument");
        }
        Boolean showKeys = (Boolean) args.get(SHOW_KEYS_ARG);

        // tags is required in args map (can be empty list)
        if (!args.containsKey(TAGS_ARG)) {
            throw new IllegalArgumentException("ShowTags stage requires '" + TAGS_ARG + "' argument");
        }
        List<String> tags = (List<String>) args.get(TAGS_ARG);

        return new ShowTagsStage(showKeys, tags != null ? tags : List.of());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ShowTagsStage that)) return false;
        return showKeys == that.showKeys && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(showKeys, tags);
    }
}
