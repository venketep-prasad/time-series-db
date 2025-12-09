/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;

import java.util.List;

public class ShowTagsPlanNodeTests extends BasePlanNodeTests {

    public void testNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("showTags");

        ShowTagsPlanNode planNode = ShowTagsPlanNode.of(functionNode);

        assertTrue(planNode.isShowKeys());
        assertTrue(planNode.getTags().isEmpty());
    }

    public void testShowKeysFalse() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("showTags");
        functionNode.addChildNode(new ValueNode("false"));

        ShowTagsPlanNode planNode = ShowTagsPlanNode.of(functionNode);

        assertFalse(planNode.isShowKeys());
        assertTrue(planNode.getTags().isEmpty());
    }

    public void testShowKeysTrueWithTags() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("showTags");
        functionNode.addChildNode(new ValueNode("true"));
        functionNode.addChildNode(new ValueNode("city"));
        functionNode.addChildNode(new ValueNode("dc"));

        ShowTagsPlanNode planNode = ShowTagsPlanNode.of(functionNode);

        assertTrue(planNode.isShowKeys());
        assertEquals(List.of("city", "dc"), planNode.getTags());
    }

    public void testOnlyTagsNoShowKeys() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("showTags");
        functionNode.addChildNode(new ValueNode("city"));
        functionNode.addChildNode(new ValueNode("dc"));

        Exception exception = assertThrows(IllegalArgumentException.class, () -> { ShowTagsPlanNode.of(functionNode); });

        assertEquals("function showTags expects argument 0 of type bool, but received 'city'", exception.getMessage());
    }

    public void testInvalidBooleanValue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("showTags");
        functionNode.addChildNode(new ValueNode("invalid"));

        Exception exception = assertThrows(IllegalArgumentException.class, () -> { ShowTagsPlanNode.of(functionNode); });

        assertEquals("function showTags expects argument 0 of type bool, but received 'invalid'", exception.getMessage());
    }

    public void testGetExplainName() {
        ShowTagsPlanNode planNode = new ShowTagsPlanNode(1, true, List.of("city", "dc"));
        String explainName = planNode.getExplainName();

        assertEquals("SHOW_TAGS(showKeys=true, tags=[city, dc])", explainName);
    }

    public void testGetExplainNameNoTags() {
        ShowTagsPlanNode planNode = new ShowTagsPlanNode(1, false, List.of());
        String explainName = planNode.getExplainName();

        assertEquals("SHOW_TAGS(showKeys=false, tags=[ALL])", explainName);
    }
}
