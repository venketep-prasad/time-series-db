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
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Pipeline stage that implements M3QL's sort function.
 *
 * These functions are used to sort time series by either avg, max, or sum of their values.
 * sort also takes an optional second argument, asc or desc respectively, which specifies
 * whether the data should be sorted in ascending or descending order.
 * If no direction is specified, the default is descending order.
 *
 * The sorting process:
 * 1. For each time series, calculate the sorting key based on max/avg/sum of all its values
 * 2. Sort the time series based on the sorting key, keeping each time series unchanged
 *
 * Usage: fetch a | sort avg desc
 *
 * Note: This is a global aggregation stage that operates on all time series at once.
 */
@PipelineStageAnnotation(name = "sort")
public class SortStage implements UnaryPipelineStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "sort";
    /** The argument name for sortBy parameter. */
    public static final String SORT_BY_ARG = "sortBy";
    /** The argument name for sortOrder parameter. */
    public static final String SORT_ORDER_ARG = "sortOrder";

    /**
     * Enumeration of supported sorting criteria for time series.
     */
    public enum SortBy {
        /** Sort by average of all values in the time series */
        AVG("avg"),
        /** Sort by maximum value in the time series */
        MAX("max"),
        /** Sort by sum of all values in the time series */
        SUM("sum");

        private final String name;

        SortBy(String name) {
            this.name = name;
        }

        /**
         * Get the string representation of this sort criteria.
         * @return the name of the sort criteria
         */
        public String getName() {
            return name;
        }

        /**
         * Parse a string into a SortBy enum value.
         * @param name the string representation (case insensitive)
         * @return the corresponding SortBy enum value
         * @throws IllegalArgumentException if the name is not recognized
         */
        public static SortBy fromString(String name) {
            for (SortBy sortBy : values()) {
                if (sortBy.name.equalsIgnoreCase(name)) {
                    return sortBy;
                }
            }
            throw new IllegalArgumentException("Unknown sort function: " + name + ". Supported: avg, max, sum");
        }
    }

    /**
     * Enumeration of supported sort orders.
     */
    public enum SortOrder {
        /** Ascending order (lowest to highest) */
        ASC("asc"),
        /** Descending order (highest to lowest) */
        DESC("desc");

        private final String name;

        SortOrder(String name) {
            this.name = name;
        }

        /**
         * Get the string representation of this sort order.
         * @return the name of the sort order
         */
        public String getName() {
            return name;
        }

        /**
         * Parse a string into a SortOrder enum value.
         * @param name the string representation (case insensitive)
         * @return the corresponding SortOrder enum value
         * @throws IllegalArgumentException if the name is not recognized
         */
        public static SortOrder fromString(String name) {
            for (SortOrder order : values()) {
                if (order.name.equalsIgnoreCase(name)) {
                    return order;
                }
            }
            throw new IllegalArgumentException("Unknown sort order: " + name + ". Supported: asc, desc");
        }
    }

    private final SortBy sortBy;
    private final SortOrder sortOrder;

    /**
     * Constructs a new SortStage with the specified sort criteria and order.
     *
     * @param sortBy the criteria to sort by (avg, max, sum)
     * @param sortOrder the order to sort in (asc, desc)
     */
    public SortStage(SortBy sortBy, SortOrder sortOrder) {
        this.sortBy = sortBy;
        this.sortOrder = sortOrder;
    }

    /**
     * Constructs a new SortStage with the specified sort criteria and default descending order.
     *
     * @param sortBy the criteria to sort by (avg, max, sum)
     */
    public SortStage(SortBy sortBy) {
        this(sortBy, SortOrder.DESC); // Default to descending order
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException("Input cannot be null");
        }
        if (input.isEmpty()) {
            return input;
        }

        // Create a copy to avoid modifying the original list
        List<TimeSeries> result = new ArrayList<>(input);

        // Sort time series based on the calculated sorting key
        // Each time series remains unchanged, only the order changes
        Comparator<TimeSeries> comparator = createComparator();

        if (sortOrder == SortOrder.ASC) {
            result.sort(comparator);
        } else {
            result.sort(comparator.reversed());
        }

        return result;
    }

    /**
     * Create a comparator that compares time series based on their sorting key.
     */
    private Comparator<TimeSeries> createComparator() {
        return switch (sortBy) {
            case AVG -> Comparator.comparingDouble(this::calculateAverage);
            case MAX -> Comparator.comparingDouble(this::calculateMax);
            case SUM -> Comparator.comparingDouble(this::calculateSum);
        };
    }

    /**
     * Calculate the average of all values in the time series as the sorting key.
     */
    private double calculateAverage(TimeSeries timeSeries) {
        List<Sample> samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return 0.0;
        }

        double sum = 0.0;
        int count = 0;
        for (Sample sample : samples) {
            if (sample != null && !Double.isNaN(sample.getValue())) {
                sum += sample.getValue();
                count++;
            }
        }

        return count == 0 ? 0.0 : sum / count;
    }

    /**
     * Calculate the maximum value in the time series as the sorting key.
     */
    private double calculateMax(TimeSeries timeSeries) {
        List<Sample> samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return Double.NEGATIVE_INFINITY;
        }

        double max = Double.NEGATIVE_INFINITY;
        for (Sample sample : samples) {
            if (sample != null && !Double.isNaN(sample.getValue())) {
                max = Math.max(max, sample.getValue());
            }
        }

        return max == Double.NEGATIVE_INFINITY ? 0.0 : max;
    }

    /**
     * Calculate the sum of all values in the time series as the sorting key.
     */
    private double calculateSum(TimeSeries timeSeries) {
        List<Sample> samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return 0.0;
        }

        double sum = 0.0;
        for (Sample sample : samples) {
            if (sample != null && !Double.isNaN(sample.getValue())) {
                sum += sample.getValue();
            }
        }

        return sum;
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Get the sort criteria.
     * @return the sort criteria
     */
    public SortBy getSortBy() {
        return sortBy;
    }

    /**
     * Get the sort order.
     * @return the sort order
     */
    public SortOrder getSortOrder() {
        return sortOrder;
    }

    @Override
    public boolean isGlobalAggregation() {
        return true;
    }

    @Override
    public boolean isCoordinatorOnly() {
        return true;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(SORT_BY_ARG, sortBy.getName());
        builder.field(SORT_ORDER_ARG, sortOrder.getName());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sortBy.getName());
        out.writeString(sortOrder.getName());
    }

    /**
     * Create a SortStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new SortStage instance with the deserialized parameters
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static SortStage readFrom(StreamInput in) throws IOException {
        String sortByName = in.readString();
        String sortOrderName = in.readString();

        SortBy sortBy = SortBy.fromString(sortByName);
        SortOrder sortOrder = SortOrder.fromString(sortOrderName);

        return new SortStage(sortBy, sortOrder);
    }

    /**
     * Create a SortStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return SortStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static SortStage fromArgs(Map<String, Object> args) {
        if (args == null || !args.containsKey(SORT_BY_ARG)) {
            throw new IllegalArgumentException("Sort stage requires '" + SORT_BY_ARG + "' argument");
        }

        Object sortByObj = args.get(SORT_BY_ARG);
        if (sortByObj == null) {
            throw new IllegalArgumentException("SortBy cannot be null");
        }

        SortBy sortBy;
        if (sortByObj instanceof String sortByStr) {
            sortBy = SortBy.fromString(sortByStr);
        } else {
            throw new IllegalArgumentException(
                "Invalid type for '" + SORT_BY_ARG + "' argument. Expected String, but got " + sortByObj.getClass().getSimpleName()
            );
        }

        SortOrder sortOrder = SortOrder.DESC; // Default
        if (args.containsKey(SORT_ORDER_ARG)) {
            Object sortOrderObj = args.get(SORT_ORDER_ARG);
            if (sortOrderObj != null) {
                if (sortOrderObj instanceof String sortOrderStr) {
                    sortOrder = SortOrder.fromString(sortOrderStr);
                } else {
                    throw new IllegalArgumentException(
                        "Invalid type for '"
                            + SORT_ORDER_ARG
                            + "' argument. Expected String, but got "
                            + sortOrderObj.getClass().getSimpleName()
                    );
                }
            }
        }

        return new SortStage(sortBy, sortOrder);
    }

    @Override
    public boolean supportConcurrentSegmentSearch() {
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SortStage other = (SortStage) obj;
        return sortBy == other.sortBy && sortOrder == other.sortOrder;
    }

    @Override
    public int hashCode() {
        int result = sortBy != null ? sortBy.hashCode() : 0;
        result = 31 * result + (sortOrder != null ? sortOrder.hashCode() : 0);
        return result;
    }
}
