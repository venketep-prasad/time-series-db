/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.plugin.rest;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Transforms OpenSearch search responses into Prometheus-compatible matrix format.
 *
 * <p>This listener processes {@link SearchResponse} objects containing time series aggregations
 * and converts them into a matrix-formatted JSON response following the Prometheus query_range API structure.
 * The matrix format is particularly useful for representing time series data with multiple data points
 * over time.</p>
 *
 * <h2>Response Structure:</h2>
 * <pre>{@code
 * {
 *   "status": "success",
 *   "data": {
 *     "resultType": "matrix",
 *     "result": [
 *       {
 *         "metric": {
 *           "__name__": "metric_name",
 *           "label1": "value1",
 *           "label2": "value2"
 *         },
 *         "values": [
 *           [timestamp1, "value1"],
 *           [timestamp2, "value2"]
 *         ]
 *       }
 *     ]
 *   }
 * }
 * }</pre>
 *
 * <h2>Error Handling:</h2>
 * <p>If an exception occurs during transformation, the listener returns a 500 error with an error response:</p>
 * <pre>{@code
 * {
 *   "status": "error",
 *   "error": "error message"
 * }
 * }</pre>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * RestChannel channel = ...;
 * String aggregationName = "timeseries_agg";
 * PromMatrixResponseListener listener = new PromMatrixResponseListener(channel, aggregationName);
 * client.search(request, listener);
 * }</pre>
 *
 * @see RestToXContentListener
 * @see InternalTimeSeries
 * @see TimeSeries
 */
public class PromMatrixResponseListener extends RestToXContentListener<SearchResponse> {

    // Response field names
    private static final String FIELD_STATUS = "status";
    private static final String FIELD_DATA = "data";
    private static final String FIELD_RESULT_TYPE = "resultType";
    private static final String FIELD_RESULT = "result";
    private static final String FIELD_ERROR = "error";
    private static final String FIELD_METRIC = "metric";
    private static final String FIELD_VALUES = "values";

    // Response status values
    private static final String STATUS_SUCCESS = "success";
    private static final String STATUS_ERROR = "error";

    // Response type values
    private static final String RESULT_TYPE_MATRIX = "matrix";

    // Prometheus label names
    private static final String LABEL_NAME = "__name__";

    private final String finalAggregationName;

    /**
     * Creates a new matrix response listener.
     *
     * @param channel the REST channel to send the response to
     * @param finalAggregationName the name of the final aggregation to extract (must not be null)
     * @throws NullPointerException if finalAggregationName is null
     */
    public PromMatrixResponseListener(RestChannel channel, String finalAggregationName) {
        super(channel);
        this.finalAggregationName = Objects.requireNonNull(finalAggregationName, "finalAggregationName cannot be null");
    }

    /**
     * Builds the REST response from a search response by transforming it into matrix format.
     *
     * <p>This method is called by the framework when a successful search response is received.
     * It transforms the response into matrix format and returns a {@link BytesRestResponse}.</p>
     *
     * @param response the search response to transform
     * @param builder the XContent builder to use for constructing the response
     * @return a REST response with the transformed data
     * @throws Exception if an error occurs during transformation
     */
    @Override
    public RestResponse buildResponse(SearchResponse response, XContentBuilder builder) throws Exception {
        try {
            transformToMatrixResponse(response, builder);
            return new BytesRestResponse(RestStatus.OK, builder);
        } catch (Exception e) {
            // Create a new builder for error response since the original builder may be in an invalid state
            XContentBuilder errorBuilder = channel.newErrorBuilder();
            return buildErrorResponse(errorBuilder, e);
        }
    }

    /**
     * Transforms a search response into matrix format and writes it to the XContent builder.
     *
     * @param response the search response containing time series aggregations
     * @param builder the XContent builder to write the matrix response to
     * @throws IOException if an I/O error occurs during writing
     */
    private void transformToMatrixResponse(SearchResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(FIELD_STATUS, STATUS_SUCCESS);
        builder.startObject(FIELD_DATA);
        builder.field(FIELD_RESULT_TYPE, RESULT_TYPE_MATRIX);
        builder.field(FIELD_RESULT, extractTimeSeriesFromAggregations(response.getAggregations()));
        builder.endObject();
        builder.endObject();
    }

    /**
     * Extracts time series data from aggregations.
     *
     * @param aggregations the aggregations to extract time series from
     * @return a list of time series in matrix format, or an empty list if no aggregations
     */
    private List<Map<String, Object>> extractTimeSeriesFromAggregations(Aggregations aggregations) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (aggregations != null) {
            for (Aggregation aggregation : aggregations) {
                extractTimeSeriesFromAggregation(aggregation, result);
            }
        }
        return result;
    }

    /**
     * Extracts time series data from a single aggregation.
     *
     * <p>This method checks if the aggregation is an {@link InternalTimeSeries} and, if so,
     * extracts its time series data. If a finalAggregationName is set, only time series from
     * aggregations with that name are extracted.</p>
     *
     * @param aggregation the aggregation to extract time series from
     * @param result the list to add extracted time series to
     */
    private void extractTimeSeriesFromAggregation(Aggregation aggregation, List<Map<String, Object>> result) {
        if (aggregation instanceof InternalTimeSeries) {
            InternalTimeSeries internalTimeSeries = (InternalTimeSeries) aggregation;
            if (shouldIncludeAggregation(internalTimeSeries)) {
                for (TimeSeries timeSeries : internalTimeSeries.getTimeSeries()) {
                    result.add(transformToMatrixFormat(timeSeries));
                }
            }
        }
    }

    /**
     * Determines whether an aggregation should be included in the result.
     *
     * @param timeSeries the time series aggregation to check
     * @return true if the aggregation should be included, false otherwise
     */
    private boolean shouldIncludeAggregation(InternalTimeSeries timeSeries) {
        return finalAggregationName.equals(timeSeries.getName());
    }

    /**
     * Transforms a single time series into matrix format.
     *
     * <p>The transformation includes:</p>
     * <ul>
     *   <li>Converting labels to a metric map with __name__ added from alias if present</li>
     *   <li>Converting samples to [timestamp, value] pairs with timestamps in seconds</li>
     * </ul>
     *
     * @param timeSeries the time series to transform
     * @return a map representing the time series in matrix format
     */
    private Map<String, Object> transformToMatrixFormat(TimeSeries timeSeries) {
        Map<String, Object> series = new HashMap<>();

        // Build metric labels
        Map<String, String> labels = buildMetricLabels(timeSeries);
        series.put(FIELD_METRIC, labels);

        // Build values array
        List<List<Object>> values = buildValuesArray(timeSeries);
        series.put(FIELD_VALUES, values);

        return series;
    }

    /**
     * Builds the metric labels map from a time series.
     *
     * <p>If the time series has an alias, it is added as the "__name__" label.
     * This follows the Prometheus convention where "__name__" represents the metric name.</p>
     *
     * @param timeSeries the time series to extract labels from
     * @return a map of metric labels
     */
    private Map<String, String> buildMetricLabels(TimeSeries timeSeries) {
        Map<String, String> labels = timeSeries.getLabels() != null ? new HashMap<>(timeSeries.getLabels().toMapView()) : new HashMap<>();

        // Add alias as __name__ if present
        if (timeSeries.getAlias() != null) {
            labels.put(LABEL_NAME, timeSeries.getAlias());
        }

        return labels;
    }

    /**
     * Builds the values array from time series samples.
     *
     * <p>Each value is represented as [timestamp, value] where:</p>
     * <ul>
     *   <li>timestamp is in seconds (converted from milliseconds)</li>
     *   <li>value is formatted as a string to preserve precision and handle special float values</li>
     * </ul>
     *
     * @param timeSeries the time series to extract samples from
     * @return a list of [timestamp, value] pairs
     */
    private List<List<Object>> buildValuesArray(TimeSeries timeSeries) {
        List<Sample> samples = timeSeries.getSamples();
        List<List<Object>> values = new ArrayList<>(samples.size());

        for (Sample sample : samples) {
            List<Object> value = new ArrayList<>(2);
            // TODO: Remove this hardcoded conversion once time units become user-configurable.
            // Currently assumes samples are in milliseconds and converts to seconds.
            value.add(sample.getTimestamp() / 1000.0);
            value.add(formatSampleValue(sample.getValue()));
            values.add(value);
        }

        return values;
    }

    /**
     * Formats a sample value according to Prometheus conventions.
     *
     * <p>Special float values are formatted as follows to match Prometheus behavior:</p>
     * <ul>
     *   <li>Positive infinity → "+Inf" (converted from Java's "Infinity")</li>
     *   <li>Negative infinity → "-Inf" (converted from Java's "-Infinity")</li>
     *   <li>All other values → string representation preserving precision</li>
     * </ul>
     *
     * @param value the sample value to format
     * @return a string representation of the value
     */
    private String formatSampleValue(double value) {
        if (value == Double.POSITIVE_INFINITY) {
            return "+Inf";
        } else if (value == Double.NEGATIVE_INFINITY) {
            return "-Inf";
        }
        return String.valueOf(value);
    }

    /**
     * Builds an error response when transformation fails.
     *
     * @param builder the XContent builder to use for the error response
     * @param error the exception that caused the error
     * @return a REST response with error details
     * @throws IOException if an I/O error occurs during writing
     */
    private RestResponse buildErrorResponse(XContentBuilder builder, Exception error) throws IOException {
        builder.startObject();
        builder.field(FIELD_STATUS, STATUS_ERROR);
        builder.field(FIELD_ERROR, error.getMessage());
        builder.endObject();
        return new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
    }
}
