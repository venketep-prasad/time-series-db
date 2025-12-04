/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import com.google.re2j.Pattern;
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
import java.util.stream.Collectors;

/**
 * A pipeline stage that excludes metrics that has a tag matches any of the regular expressions.
 * Takes a metric or a wildcard seriesList, followed by a regular expression(s).
 */
@PipelineStageAnnotation(name = ExcludeByTagStage.NAME)
public class ExcludeByTagStage implements UnaryPipelineStage {

    /** The name identifier for this pipeline stage. */
    public static final String NAME = "exclude_by_tag";
    private static final String TAG_NAME_ARG = "tag_name";
    private static final String PATTERNS_ARG = "patterns";

    private final String tagName;
    private final List<String> patterns;
    private final List<Pattern> compiledPatterns;

    public ExcludeByTagStage(String tagName, List<String> pattern) {
        this.tagName = tagName;
        this.patterns = pattern;
        this.compiledPatterns = pattern.stream().map(Pattern::compile).collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        if (compiledPatterns.isEmpty()) {
            throw new IllegalArgumentException("ExcludeByTag stage requires '" + PATTERNS_ARG + "' argument");
        }
        List<TimeSeries> result = new ArrayList<>(input.size());
        for (TimeSeries ts : input) {
            boolean shouldExclude = false;
            Labels seriesLabels = ts.getLabels();
            if (seriesLabels != null && seriesLabels.has(tagName)) {
                String labelValue = seriesLabels.get(tagName);
                for (Pattern compiledPattern : compiledPatterns) {
                    if (compiledPattern.matches(labelValue)) {
                        shouldExclude = true;
                        break;
                    }
                }
            }
            if (!shouldExclude) {
                result.add(ts);
            }
        }
        return result;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(TAG_NAME_ARG, tagName);
        builder.field(PATTERNS_ARG, patterns);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(tagName);
        out.writeStringCollection(patterns);
    }

    /**
     * Create an ExcludeByTagStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new ExcludeByTagStage instance
     * @throws IOException if an I/O error occurs while reading
     */
    public static ExcludeByTagStage readFrom(StreamInput in) throws IOException {
        String tagName = in.readString();
        List<String> patterns = in.readStringList();
        return new ExcludeByTagStage(tagName, patterns);
    }

    public static ExcludeByTagStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }

        if (!args.containsKey(TAG_NAME_ARG)) {
            throw new IllegalArgumentException("ExcludeByTag stage requires '" + TAG_NAME_ARG + "' argument");
        }
        String tagName = (String) args.get(TAG_NAME_ARG);
        if (tagName == null || tagName.isEmpty()) {
            throw new IllegalArgumentException("Tag cannot be null");
        }

        if (!args.containsKey(PATTERNS_ARG)) {
            throw new IllegalArgumentException("ExcludeByTag stage requires '" + PATTERNS_ARG + "' argument");
        }
        List<String> patterns = (List<String>) args.get(PATTERNS_ARG);
        if (patterns == null || patterns.isEmpty()) {
            throw new IllegalArgumentException("Patterns cannot be null");
        }

        return new ExcludeByTagStage(tagName, patterns);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ExcludeByTagStage that = (ExcludeByTagStage) obj;
        return Objects.equals(tagName, that.tagName) && Objects.equals(patterns, that.patterns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tagName, patterns);
    }

}
