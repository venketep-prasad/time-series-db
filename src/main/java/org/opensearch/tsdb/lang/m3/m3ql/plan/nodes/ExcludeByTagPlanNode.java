/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.Utils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * ExcludeByTagPlanNode represents a node in the M3QL plan that exclude tag by regular expression patterns.
 * Takes 1 tag name and multiple patterns. If a tag value is not found, it is ignored.
 */
public class ExcludeByTagPlanNode extends M3PlanNode {
    private final String tagName;
    private final List<String> patterns;

    /**
     * Constructor for ExcludeByTagPlanNode
     * @param id
     * @param tagName
     * @param patterns
     */
    public ExcludeByTagPlanNode(int id, String tagName, List<String> patterns) {
        super(id);
        this.tagName = tagName;
        this.patterns = patterns;
    }

    public String getTagName() {
        return tagName;
    }

    public List<String> getPatterns() {
        return new ArrayList<>(patterns);
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "EXCLUDE_BY_TAG(tag=%s,patterns=%s)", tagName, String.join(", ", patterns));
    }

    /**
     * Factory method to create an ExcludeByTagPlanNode from a FunctionNode.
     * Expects the function node to have children that are ValueNodes representing tag name and patterns.
     *
     * @param functionNode the function node to convert
     * @return an instance of ExcludeByTagPlanNode
     */
    public static ExcludeByTagPlanNode of(FunctionNode functionNode) {
        int argCount = functionNode.getChildren().size();
        if (argCount < 2) {
            throw new IllegalArgumentException("ExcludeByTag function must specify tag and pattern to exclude");
        }
        // Parse tag (first argument)
        if (!(functionNode.getChildren().get(0) instanceof ValueNode tagNode)) {
            throw new IllegalArgumentException("First argument must be a value representing tag");
        }
        String tagName = Utils.stripDoubleQuotes(tagNode.getValue());

        // Parse patterns
        List<String> patterns = new ArrayList<>();
        for (int i = 1; i < argCount; i++) {
            if (!(functionNode.getChildren().get(i) instanceof ValueNode valueNode)) {
                throw new IllegalArgumentException("patterns argument must be a value representing the regular expression");
            }
            String pattern = Utils.stripDoubleQuotes(valueNode.getValue());
            patterns.add(pattern);
        }
        return new ExcludeByTagPlanNode(M3PlannerContext.generateId(), tagName, patterns);
    }
}
