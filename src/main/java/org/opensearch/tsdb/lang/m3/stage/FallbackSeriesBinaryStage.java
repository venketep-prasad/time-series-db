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
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.BinaryPipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * FallbackSeriesBinaryStage returns its left input series if it is not empty,
 * otherwise it returns the right input series (the fallback).
 *
 * <p>This is the binary variant of fallbackSeries that takes a pipeline as replacement.
 * Example: {@code fetch a | fallbackSeries(fetch b)}
 *
 * <p>This stage is useful in auto-generated alerts where the input series may not always exist.
 */
@PipelineStageAnnotation(name = FallbackSeriesBinaryStage.NAME)
public class FallbackSeriesBinaryStage implements BinaryPipelineStage {
    /** The name of this pipeline stage. */
    public static final String NAME = "fallbackSeriesBinary";

    private final String rightOperandReferenceName;

    /**
     * Constructor for FallbackSeriesBinaryStage.
     *
     * @param rightOperandReferenceName the reference name for the right operand
     */
    public FallbackSeriesBinaryStage(String rightOperandReferenceName) {
        this.rightOperandReferenceName = rightOperandReferenceName;
    }

    /**
     * Constructor for FallbackSeriesBinaryStage with stream input.
     *
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public FallbackSeriesBinaryStage(StreamInput in) throws IOException {
        this.rightOperandReferenceName = in.readString();
    }

    /**
     * Process two lists of time series by returning left if non-empty, otherwise right.
     *
     * @param left the left operand time series list
     * @param right the right operand time series list (fallback)
     * @return left series if non-empty, otherwise right series
     */
    @Override
    public List<TimeSeries> process(List<TimeSeries> left, List<TimeSeries> right) {
        // If left series is empty, return right (fallback), otherwise return left
        if (left == null || left.isEmpty()) {
            return right;
        }
        return left;
    }

    /**
     * Get the reference name for the right operand.
     *
     * @return the right operand reference name
     */
    @Override
    public String getRightOpReferenceName() {
        return rightOperandReferenceName;
    }

    /**
     * Get the name of this pipeline stage.
     *
     * @return the stage name "fallbackSeriesBinary"
     */
    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Serialize this stage to XContent format.
     *
     * @param builder the XContent builder to write to
     * @param params the XContent parameters
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(RIGHT_OP_REFERENCE_PARAM_KEY, rightOperandReferenceName);
    }

    /**
     * Write this stage to a stream for serialization.
     *
     * @param out the stream output to write to
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(rightOperandReferenceName);
    }

    /**
     * Creates a FallbackSeriesBinaryStage from DSL arguments.
     *
     * @param args the arguments map containing right_op_reference
     * @return a new FallbackSeriesBinaryStage instance
     */
    public static FallbackSeriesBinaryStage fromArgs(Map<String, Object> args) {
        if (args == null || !args.containsKey(RIGHT_OP_REFERENCE_PARAM_KEY)) {
            throw new IllegalArgumentException("fallbackSeriesBinary stage requires " + RIGHT_OP_REFERENCE_PARAM_KEY + " argument");
        }
        String rightOpRef = (String) args.get(RIGHT_OP_REFERENCE_PARAM_KEY);
        return new FallbackSeriesBinaryStage(rightOpRef);
    }

    /**
     * Reads a FallbackSeriesBinaryStage from a stream.
     *
     * @param in the stream input
     * @return a new FallbackSeriesBinaryStage instance
     * @throws IOException if an I/O error occurs
     */
    public static FallbackSeriesBinaryStage readFrom(StreamInput in) throws IOException {
        return new FallbackSeriesBinaryStage(in);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FallbackSeriesBinaryStage that = (FallbackSeriesBinaryStage) obj;
        return rightOperandReferenceName != null
            ? rightOperandReferenceName.equals(that.rightOperandReferenceName)
            : that.rightOperandReferenceName == null;
    }

    @Override
    public int hashCode() {
        return rightOperandReferenceName != null ? rightOperandReferenceName.hashCode() : 0;
    }
}
