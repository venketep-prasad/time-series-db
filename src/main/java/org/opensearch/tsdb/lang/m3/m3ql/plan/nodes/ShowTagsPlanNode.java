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
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * ShowTagsPlanNode represents a node in the M3QL plan that formats series aliases from tag key-value pairs.
 *
 * <p>Grammar: showTags([showKeys], [tag...])
 * - showKeys: optional boolean to show/hide tag keys (default: true)
 * - tag: optional variadic list of tag names to display
 *
 * <p>Behavior:
 * - If no tags specified: shows all available tags (sorted alphabetically)
 * - If tags specified: shows only those tags in the order provided
 * - showKeys=true format: "tag1:value1 tag2:value2"
 * - showKeys=false format: "value1 value2"
 * - Missing tags are skipped (only existing tags are shown)
 */
public class ShowTagsPlanNode extends M3PlanNode {
    private final boolean showKeys;
    private final List<String> tags;

    /**
     * Constructor for ShowTagsPlanNode.
     *
     * @param id node id
     * @param showKeys whether to show tag keys in the output
     * @param tags the list of tag names to display (empty list means show all tags)
     */
    public ShowTagsPlanNode(int id, boolean showKeys, List<String> tags) {
        super(id);
        this.showKeys = showKeys;
        this.tags = new ArrayList<>(tags);
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        String tagsStr = tags.isEmpty() ? "ALL" : String.join(", ", tags);
        return String.format(Locale.ROOT, "SHOW_TAGS(showKeys=%s, tags=[%s])", showKeys, tagsStr);
    }

    /**
     * Returns whether to show tag keys in the output.
     * @return true if tag keys should be shown, false otherwise
     */
    public boolean isShowKeys() {
        return showKeys;
    }

    /**
     * Returns the list of tag names to display.
     * @return List of tag names (empty list means show all tags)
     */
    public List<String> getTags() {
        return new ArrayList<>(tags);
    }

    /**
     * Factory method to create a ShowTagsPlanNode from a FunctionNode.
     * Expects optional arguments in order:
     * 1. showKeys (boolean, optional, default: true)
     * 2. tag names (variadic string, optional)
     *
     * @param functionNode the function node to convert
     * @return an instance of ShowTagsPlanNode
     */
    public static ShowTagsPlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> children = functionNode.getChildren();
        int argCount = children.size();

        // Default values
        boolean showKeys = true;
        int tagStartIndex = 0;

        // Check if first argument is a boolean (showKeys parameter)
        if (argCount > 0 && children.get(0) instanceof ValueNode firstArg) {
            String firstValue = firstArg.getValue().toLowerCase(Locale.ROOT);

            // Check if it's a boolean value
            if ("true".equals(firstValue) || "false".equals(firstValue)) {
                showKeys = Boolean.parseBoolean(firstValue);
                tagStartIndex = 1;
            } else {
                throw new IllegalArgumentException("function showTags expects argument 0 of type bool, but received '" + firstValue + "'");
            }
            // Otherwise, first argument is a tag name, use default showKeys=true
        }

        // Parse variadic tag names (remaining arguments)
        List<String> tags = new ArrayList<>();
        for (int i = tagStartIndex; i < argCount; i++) {
            if (!(children.get(i) instanceof ValueNode valueNode)) {
                throw new IllegalArgumentException("showTags tag arguments must be string values");
            }
            String tagName = Utils.stripDoubleQuotes(valueNode.getValue());
            tags.add(tagName);
        }

        return new ShowTagsPlanNode(M3PlannerContext.generateId(), showKeys, tags);
    }
}
