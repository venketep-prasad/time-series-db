/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.promql.plan.nodes;

import org.opensearch.tsdb.lang.prom.common.MatcherType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Plan node representing a fetch operation (vector or matrix selector).
 */
public class FetchPlanNode extends PromPlanNode {
    private final String metricName;
    private final Map<String, LabelMatcherInfo> labelMatchers;
    private final Long rangeMs;  // null for instant vector, non-null for range vector

    public FetchPlanNode(int id, String metricName, Long rangeMs) {
        super(id);
        this.metricName = metricName;
        this.rangeMs = rangeMs;
        this.labelMatchers = new LinkedHashMap<>();
    }

    public String getMetricName() {
        return metricName;
    }

    public Long getRangeMs() {
        return rangeMs;
    }

    public boolean isRangeVector() {
        return rangeMs != null;
    }

    public void addLabelMatcher(String labelName, MatcherType matcherType, String value) {
        labelMatchers.put(labelName, new LabelMatcherInfo(labelName, matcherType, value));
    }

    public Map<String, LabelMatcherInfo> getLabelMatchers() {
        return labelMatchers;
    }

    @Override
    public <T> T accept(PromPlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "Fetch";
    }

    /**
     * Information about a label matcher.
     */
    public static class LabelMatcherInfo {
        private final String labelName;
        private final MatcherType matcherType;
        private final String value;

        public LabelMatcherInfo(String labelName, MatcherType matcherType, String value) {
            this.labelName = labelName;
            this.matcherType = matcherType;
            this.value = value;
        }

        public String getLabelName() {
            return labelName;
        }

        public MatcherType getMatcherType() {
            return matcherType;
        }

        public String getValue() {
            return value;
        }
    }
}
