/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.promql.parser.nodes;

import org.opensearch.tsdb.lang.prom.common.MatcherType;

/**
 * Represents a label matcher in PromQL.
 * Examples: job="api", method!="POST", instance=~".*"
 */
public class LabelMatcherNode extends PromASTNode {
    /**
     * The label name.
     */
    private final String labelName;

    /**
     * The matcher type (EQUAL, NOT_EQUAL, REGEX_MATCH, REGEX_NOT_MATCH).
     */
    private final MatcherType matcherType;

    /**
     * The value to match against.
     */
    private final String value;

    /**
     * Constructor for LabelMatcherNode.
     * @param labelName the label name
     * @param matcherType the matcher type
     * @param value the value to match against
     */
    public LabelMatcherNode(String labelName, MatcherType matcherType, String value) {
        super();
        this.labelName = labelName;
        this.matcherType = matcherType;
        this.value = value;
    }

    /**
     * Gets the label name.
     * @return the label name
     */
    public String getLabelName() {
        return labelName;
    }

    /**
     * Gets the matcher type.
     * @return the matcher type
     */
    public MatcherType getMatcherType() {
        return matcherType;
    }

    /**
     * Gets the value to match against.
     * @return the value
     */
    public String getValue() {
        return value;
    }

    @Override
    public <T> T accept(PromASTVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
