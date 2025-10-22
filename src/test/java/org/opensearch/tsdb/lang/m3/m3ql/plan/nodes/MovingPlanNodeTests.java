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
 * Unit tests for MovingPlanNode.
 */
public class MovingPlanNodeTests extends BasePlanNodeTests {

    public void testMovingPlanNodeCreationWithTimeBased() {
        MovingPlanNode node = new MovingPlanNode(1, "5m", WindowAggregationType.AVG);

        assertEquals(1, node.getId());
        assertEquals(Duration.ofMinutes(5), node.getTimeDuration());
        assertEquals(WindowAggregationType.AVG, node.getAggregationType());
        assertEquals("MOVING(5m, AVG)", node.getExplainName());
        assertFalse(node.isPointBased());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testMovingPlanNodeCreationWithPointBased() {
        MovingPlanNode node = new MovingPlanNode(1, "10", WindowAggregationType.SUM);

        assertTrue(node.isPointBased());
        assertEquals(10, node.getPointDuration().intValue());
        assertEquals(WindowAggregationType.SUM, node.getAggregationType());
        assertEquals("MOVING(10, SUM)", node.getExplainName());
    }

    public void testMovingPlanNodeVisitorAccept() {
        MovingPlanNode node = new MovingPlanNode(1, "1h", WindowAggregationType.MAX);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit MovingPlanNode", result);
    }

    public void testMovingPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("moving");
        functionNode.addChildNode(new ValueNode("5m"));
        functionNode.addChildNode(new ValueNode("avg"));

        MovingPlanNode node = MovingPlanNode.of(functionNode);

        assertEquals(Duration.ofMinutes(5), node.getTimeDuration());
        assertEquals(WindowAggregationType.AVG, node.getAggregationType());
        assertFalse(node.isPointBased());
    }

    public void testMovingPlanNodeFactoryMethodWithPointBased() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("moving");
        functionNode.addChildNode(new ValueNode("20"));
        functionNode.addChildNode(new ValueNode("max"));

        MovingPlanNode node = MovingPlanNode.of(functionNode);

        assertTrue(node.isPointBased());
        assertEquals(20, node.getPointDuration().intValue());
        assertEquals(WindowAggregationType.MAX, node.getAggregationType());
    }

    public void testMovingPlanNodeWithDifferentTimeUnits() {
        MovingPlanNode hourNode = new MovingPlanNode(1, "2h", WindowAggregationType.MIN);
        assertEquals(Duration.ofHours(2), hourNode.getTimeDuration());

        MovingPlanNode dayNode = new MovingPlanNode(2, "1d", WindowAggregationType.SUM);
        assertEquals(Duration.ofDays(1), dayNode.getTimeDuration());

        MovingPlanNode secondNode = new MovingPlanNode(3, "30s", WindowAggregationType.AVG);
        assertEquals(Duration.ofSeconds(30), secondNode.getTimeDuration());
    }

    public void testMovingPlanNodeIsPointBasedDetection() {
        assertTrue(new MovingPlanNode(1, "5", WindowAggregationType.SUM).isPointBased());
        assertTrue(new MovingPlanNode(1, "100", WindowAggregationType.AVG).isPointBased());
        assertTrue(new MovingPlanNode(1, " 50 ", WindowAggregationType.MAX).isPointBased());

        assertFalse(new MovingPlanNode(1, "5m", WindowAggregationType.SUM).isPointBased());
        assertFalse(new MovingPlanNode(1, "1h", WindowAggregationType.AVG).isPointBased());
        assertFalse(new MovingPlanNode(1, "2d", WindowAggregationType.MAX).isPointBased());
        assertFalse(new MovingPlanNode(1, "5.5", WindowAggregationType.MIN).isPointBased());
    }

    public void testMovingPlanNodeFactoryMethodThrowsOnIncorrectArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("moving");
        functionNode.addChildNode(new ValueNode("5m"));

        expectThrows(IllegalArgumentException.class, () -> MovingPlanNode.of(functionNode));
    }

    public void testMovingPlanNodeFactoryMethodThrowsOnTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("moving");
        functionNode.addChildNode(new ValueNode("5m"));
        functionNode.addChildNode(new ValueNode("avg"));
        functionNode.addChildNode(new ValueNode("extra"));

        expectThrows(IllegalArgumentException.class, () -> MovingPlanNode.of(functionNode));
    }

    public void testMovingPlanNodeFactoryMethodThrowsOnNonValueNodes() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("moving");
        functionNode.addChildNode(new FunctionNode()); // not a value node
        functionNode.addChildNode(new ValueNode("avg"));

        expectThrows(IllegalArgumentException.class, () -> MovingPlanNode.of(functionNode));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(MovingPlanNode planNode) {
            return "visit MovingPlanNode";
        }
    }
}
