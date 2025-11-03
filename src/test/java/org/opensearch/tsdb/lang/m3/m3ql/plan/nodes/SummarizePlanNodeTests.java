/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.WindowAggregationType;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.time.Duration;

/**
 * Unit tests for SummarizePlanNode.
 */
public class SummarizePlanNodeTests extends BasePlanNodeTests {

    public void testSummarizePlanNodeCreation() {
        SummarizePlanNode node = new SummarizePlanNode(1, "5m", WindowAggregationType.SUM, false);

        assertEquals(1, node.getId());
        assertEquals(Duration.ofMinutes(5), node.getInterval());
        assertEquals(WindowAggregationType.SUM, node.getFunction());
        assertFalse(node.isAlignToFrom());
        assertEquals("SUMMARIZE(5m, SUM, false)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testSummarizePlanNodeCreationWithAlignToFrom() {
        SummarizePlanNode node = new SummarizePlanNode(1, "1h", WindowAggregationType.AVG, true);

        assertEquals(Duration.ofHours(1), node.getInterval());
        assertEquals(WindowAggregationType.AVG, node.getFunction());
        assertTrue(node.isAlignToFrom());
        assertEquals("SUMMARIZE(1h, AVG, true)", node.getExplainName());
    }

    public void testSummarizePlanNodeVisitorAccept() {
        SummarizePlanNode node = new SummarizePlanNode(1, "10m", WindowAggregationType.MAX, false);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit SummarizePlanNode", result);
    }

    public void testSummarizePlanNodeFactoryMethodWithTwoArgs() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("summarize");
        functionNode.addChildNode(new ValueNode("5m"));
        functionNode.addChildNode(new ValueNode("avg"));

        SummarizePlanNode node = SummarizePlanNode.of(functionNode);

        assertEquals(Duration.ofMinutes(5), node.getInterval());
        assertEquals(WindowAggregationType.AVG, node.getFunction());
        assertFalse(node.isAlignToFrom());
    }

    public void testSummarizePlanNodeFactoryMethodWithThreeArgs() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("summarize");
        functionNode.addChildNode(new ValueNode("1h"));
        functionNode.addChildNode(new ValueNode("max"));
        functionNode.addChildNode(new ValueNode("true"));

        SummarizePlanNode node = SummarizePlanNode.of(functionNode);

        assertEquals(Duration.ofHours(1), node.getInterval());
        assertEquals(WindowAggregationType.MAX, node.getFunction());
        assertTrue(node.isAlignToFrom());
    }

    public void testSummarizePlanNodeFactoryMethodWithAlignToFromFalse() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("summarize");
        functionNode.addChildNode(new ValueNode("30s"));
        functionNode.addChildNode(new ValueNode("sum"));
        functionNode.addChildNode(new ValueNode("false"));

        SummarizePlanNode node = SummarizePlanNode.of(functionNode);

        assertEquals(Duration.ofSeconds(30), node.getInterval());
        assertEquals(WindowAggregationType.SUM, node.getFunction());
        assertFalse(node.isAlignToFrom());
    }

    public void testSummarizePlanNodeWithDifferentTimeUnits() {
        SummarizePlanNode hourNode = new SummarizePlanNode(1, "2h", WindowAggregationType.MIN, false);
        assertEquals(Duration.ofHours(2), hourNode.getInterval());

        SummarizePlanNode dayNode = new SummarizePlanNode(2, "1d", WindowAggregationType.SUM, false);
        assertEquals(Duration.ofDays(1), dayNode.getInterval());

        SummarizePlanNode secondNode = new SummarizePlanNode(3, "30s", WindowAggregationType.AVG, true);
        assertEquals(Duration.ofSeconds(30), secondNode.getInterval());
    }

    public void testSummarizePlanNodeWithPercentileFunction() {
        SummarizePlanNode node = new SummarizePlanNode(1, "5m", WindowAggregationType.withPercentile(95), false);

        assertEquals(Duration.ofMinutes(5), node.getInterval());
        assertEquals(WindowAggregationType.withPercentile(95), node.getFunction());
        assertTrue(node.getFunction().isPercentile());
        assertEquals(95.0f, node.getFunction().getPercentileValue(), 0.001);
    }

    public void testSummarizePlanNodeFactoryMethodWithPercentile() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("summarize");
        functionNode.addChildNode(new ValueNode("5m"));
        functionNode.addChildNode(new ValueNode("p99"));

        SummarizePlanNode node = SummarizePlanNode.of(functionNode);

        assertEquals(Duration.ofMinutes(5), node.getInterval());
        assertTrue(node.getFunction().isPercentile());
        assertEquals(99.0f, node.getFunction().getPercentileValue(), 0.001);
    }

    public void testSummarizePlanNodeFactoryMethodWithOneArgument() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("summarize");
        functionNode.addChildNode(new ValueNode("5m"));

        SummarizePlanNode node = SummarizePlanNode.of(functionNode);

        assertEquals(Duration.ofMinutes(5), node.getInterval());
        assertEquals(WindowAggregationType.SUM, node.getFunction()); // default to sum
        assertFalse(node.isAlignToFrom()); // default to false
    }

    public void testSummarizePlanNodeFactoryMethodThrowsOnTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("summarize");
        functionNode.addChildNode(new ValueNode("5m"));
        functionNode.addChildNode(new ValueNode("avg"));
        functionNode.addChildNode(new ValueNode("true"));
        functionNode.addChildNode(new ValueNode("extra"));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> SummarizePlanNode.of(functionNode));
        assertEquals("Summarize function must have 1-3 arguments: interval, [function], [alignToFrom]. Got: 4", exception.getMessage());
    }

    public void testSummarizePlanNodeFactoryMethodThrowsOnNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("summarize");
        // No arguments added

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> SummarizePlanNode.of(functionNode));
        assertEquals("Summarize function must have 1-3 arguments: interval, [function], [alignToFrom]. Got: 0", exception.getMessage());
    }

    public void testSummarizePlanNodeFactoryMethodThrowsOnNonValueNodes() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("summarize");
        functionNode.addChildNode(new FunctionNode()); // not a value node
        functionNode.addChildNode(new ValueNode("avg"));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> SummarizePlanNode.of(functionNode));
        assertEquals("First argument must be a value representing the interval", exception.getMessage());
    }

    public void testSummarizePlanNodeFactoryMethodThrowsOnNonValueNodeFunction() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("summarize");
        functionNode.addChildNode(new ValueNode("5m"));
        functionNode.addChildNode(new FunctionNode()); // Second argument is not a ValueNode

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> SummarizePlanNode.of(functionNode));
        assertEquals("Second argument must be a value representing the aggregation function", exception.getMessage());
    }

    public void testSummarizePlanNodeFactoryMethodThrowsOnInvalidAlignToFrom() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("summarize");
        functionNode.addChildNode(new ValueNode("5m"));
        functionNode.addChildNode(new ValueNode("avg"));
        functionNode.addChildNode(new ValueNode("invalid"));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> SummarizePlanNode.of(functionNode));
        assertEquals("Third argument (alignToFrom) must be 'true' or 'false', got: invalid", exception.getMessage());
    }

    public void testSummarizePlanNodeFactoryMethodThrowsOnNonValueNodeAlignToFrom() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("summarize");
        functionNode.addChildNode(new ValueNode("5m"));
        functionNode.addChildNode(new ValueNode("avg"));
        functionNode.addChildNode(new FunctionNode()); // Third argument is not a ValueNode

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> SummarizePlanNode.of(functionNode));
        assertEquals("Third argument must be a boolean value representing alignToFrom", exception.getMessage());
    }

    public void testSummarizePlanNodeGetInvalidInterval() {
        SummarizePlanNode node = new SummarizePlanNode(1, "-5m", WindowAggregationType.AVG, false);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, node::getInterval);
        assertEquals("Interval cannot be negative: -5m", exception.getMessage());
    }

    public void testSummarizePlanNodeChildrenManagement() {
        SummarizePlanNode node = new SummarizePlanNode(1, "5m", WindowAggregationType.SUM, false);
        assertTrue(node.getChildren().isEmpty());

        FetchPlanNode fetchNode = new FetchPlanNode(2, null, null);
        node.addChild(fetchNode);

        assertEquals(1, node.getChildren().size());
        assertEquals(fetchNode, node.getChildren().get(0));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(SummarizePlanNode planNode) {
            return "visit SummarizePlanNode";
        }
    }
}
