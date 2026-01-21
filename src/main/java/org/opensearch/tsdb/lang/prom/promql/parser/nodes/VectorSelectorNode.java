/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.promql.parser.nodes;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for vector selectors in PromQL.
 * Contains common properties for both instant and range vector selectors.
 */
public abstract class VectorSelectorNode extends PromASTNode {
    /**
     * The metric name, or null if this is a label-only selector.
     */
    private final String metricName;

    /**
     * List of label matchers for filtering time series.
     */
    private final List<LabelMatcherNode> matchers;

    /**
     * Constructor for VectorSelectorNode.
     * @param metricName the name of the metric to select
     */
    protected VectorSelectorNode(String metricName) {
        super();
        this.metricName = metricName;
        this.matchers = new ArrayList<>();
    }

    /**
     * Gets the metric name.
     * @return the metric name
     */
    public String getMetricName() {
        return metricName;
    }

    /**
     * Adds a label matcher to this vector selector.
     * @param matcher the label matcher to add
     */
    public void addMatcher(LabelMatcherNode matcher) {
        matchers.add(matcher);
    }

    /**
     * Gets all label matchers for this selector.
     * @return the list of label matchers
     */
    public List<LabelMatcherNode> getMatchers() {
        return matchers;
    }
}
