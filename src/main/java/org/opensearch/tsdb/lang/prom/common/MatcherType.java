/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.common;

/**
 * Label matcher types in PromQL.
 */
public enum MatcherType {
    EQUAL("="),
    NOT_EQUAL("!="),
    REGEX_MATCH("=~"),
    REGEX_NOT_MATCH("!~");

    private final String operator;

    MatcherType(String operator) {
        this.operator = operator;
    }

    /**
     * Gets the operator string.
     * @return the operator string
     */
    public String getOperator() {
        return operator;
    }

    /**
     * Parse matcher type from operator string.
     * @param operator the operator string
     * @return the corresponding matcher type
     */
    public static MatcherType fromOperator(String operator) {
        for (MatcherType type : values()) {
            if (type.operator.equals(operator)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown matcher operator: " + operator);
    }
}
