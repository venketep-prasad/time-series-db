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
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Unit tests for FallbackSeriesConstantPlanNode.
 */
public class FallbackSeriesConstantPlanNodeTests extends BasePlanNodeTests {

    public void testFallbackSeriesConstantPlanNodeCreation() {
        FallbackSeriesConstantPlanNode node = new FallbackSeriesConstantPlanNode(1, 10.5);

        assertEquals(1, node.getId());
        assertEquals(10.5, node.getConstantValue(), 0.0);
        assertEquals("FALLBACK_SERIES(10.5)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testFallbackSeriesConstantPlanNodeWithNegativeValue() {
        FallbackSeriesConstantPlanNode node = new FallbackSeriesConstantPlanNode(1, -5.0);

        assertEquals(-5.0, node.getConstantValue(), 0.0);
        assertEquals("FALLBACK_SERIES(-5.0)", node.getExplainName());
    }

    public void testFallbackSeriesConstantPlanNodeWithZeroValue() {
        FallbackSeriesConstantPlanNode node = new FallbackSeriesConstantPlanNode(1, 0.0);

        assertEquals(0.0, node.getConstantValue(), 0.0);
        assertEquals("FALLBACK_SERIES(0.0)", node.getExplainName());
    }

    public void testFallbackSeriesConstantPlanNodeVisitorAccept() {
        FallbackSeriesConstantPlanNode node = new FallbackSeriesConstantPlanNode(1, 42.0);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit FallbackSeriesConstantPlanNode", result);
    }

    public void testFallbackSeriesConstantPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fallbackSeries");
        functionNode.addChildNode(new ValueNode("1.5"));

        FallbackSeriesConstantPlanNode node = FallbackSeriesConstantPlanNode.of(functionNode);

        assertEquals(1.5, node.getConstantValue(), 0.0);
    }

    public void testFallbackSeriesConstantPlanNodeFactoryMethodWithIntegerValue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fallbackSeries");
        functionNode.addChildNode(new ValueNode("100"));

        FallbackSeriesConstantPlanNode node = FallbackSeriesConstantPlanNode.of(functionNode);

        assertEquals(100.0, node.getConstantValue(), 0.0);
    }

    public void testFallbackSeriesConstantPlanNodeFactoryMethodWithNegativeValue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fallbackSeries");
        functionNode.addChildNode(new ValueNode("-2.5"));

        FallbackSeriesConstantPlanNode node = FallbackSeriesConstantPlanNode.of(functionNode);

        assertEquals(-2.5, node.getConstantValue(), 0.0);
    }

    public void testFallbackSeriesConstantPlanNodeFactoryMethodThrowsOnNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fallbackSeries");

        expectThrows(IllegalArgumentException.class, () -> FallbackSeriesConstantPlanNode.of(functionNode));
    }

    public void testFallbackSeriesConstantPlanNodeFactoryMethodThrowsOnMultipleArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fallbackSeries");
        functionNode.addChildNode(new ValueNode("1.0"));
        functionNode.addChildNode(new ValueNode("2.0"));

        expectThrows(IllegalArgumentException.class, () -> FallbackSeriesConstantPlanNode.of(functionNode));
    }

    public void testFallbackSeriesConstantPlanNodeFactoryMethodThrowsOnNonValueNode() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fallbackSeries");
        functionNode.addChildNode(new FunctionNode()); // not a value node

        expectThrows(IllegalArgumentException.class, () -> FallbackSeriesConstantPlanNode.of(functionNode));
    }

    public void testFallbackSeriesConstantPlanNodeFactoryMethodThrowsOnInvalidNumber() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fallbackSeries");
        functionNode.addChildNode(new ValueNode("not_a_number"));

        expectThrows(NumberFormatException.class, () -> FallbackSeriesConstantPlanNode.of(functionNode));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(FallbackSeriesConstantPlanNode planNode) {
            return "visit FallbackSeriesConstantPlanNode";
        }
    }
}
