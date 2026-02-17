/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.translators;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;

/**
 * Enum representing supported query types for time series queries
 */
public enum QueryType {
    M3QL("m3ql", "/_m3ql"),
    PROMQL("promql", "/_promql"),
    DSL("dsl", "/_search");

    private final String value;
    private final String restEndpoint;

    QueryType(String value, String restEndpoint) {
        this.value = value;
        this.restEndpoint = restEndpoint;
    }

    /**
     * Get the string value of the query type
     * Used for JSON serialization
     */
    @JsonValue
    public String getValue() {
        return value;
    }

    /**
     * Get the REST endpoint for this query type
     */
    public String getRestEndpoint() {
        return restEndpoint;
    }

    /**
     * Create QueryType from string value
     * Used for JSON deserialization
     *
     * @param value The string value (e.g., "m3ql", "promql")
     * @return The corresponding QueryType enum
     * @throws IllegalArgumentException if value doesn't match any QueryType
     */
    @JsonCreator
    public static QueryType fromValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Query type cannot be null or empty");
        }

        String normalizedValue = value.toLowerCase(Locale.ROOT).trim();
        for (QueryType type : QueryType.values()) {
            if (type.value.equals(normalizedValue)) {
                return type;
            }
        }

        throw new IllegalArgumentException(
            String.format(Locale.ROOT, "Unsupported query type: '%s'. Supported types: %s", value, getSupportedTypes())
        );
    }

    /**
     * Get a comma-separated string of supported query types
     */
    public static String getSupportedTypes() {
        StringBuilder sb = new StringBuilder();
        for (QueryType type : QueryType.values()) {
            if (!sb.isEmpty()) {
                sb.append(", ");
            }
            sb.append(type.value);
        }
        return sb.toString();
    }

    /**
     * Create a QueryConfigTranslator instance for this query type.
     * This translator can be used in both REST tests and internal cluster tests
     * since it returns SearchRequest.
     */
    public QueryConfigTranslator createTranslator() {
        return switch (this) {
            case M3QL -> new M3QLTranslator();
            case PROMQL -> new PromQLTranslator();
            case DSL -> new DSLTranslator();
        };
    }

    @Override
    public String toString() {
        return value;
    }
}
