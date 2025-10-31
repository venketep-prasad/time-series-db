/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Unit tests for BinaryPlanNode.
 */
public class BinaryPlanNodeTests extends BasePlanNodeTests {

    public void testBinaryPlanNodeCreation() {
        BinaryPlanNode node = new BinaryPlanNode(1, BinaryPlanNode.Type.AS_PERCENT);

        assertEquals(1, node.getId());
        assertEquals(BinaryPlanNode.Type.AS_PERCENT, node.getType());
        assertEquals("AS_PERCENT", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testBinaryPlanNodeVisitorAccept() {
        BinaryPlanNode node = new BinaryPlanNode(1, BinaryPlanNode.Type.DIFF);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit BinaryPlanNode", result);
    }

    public void testAllBinaryPlanNodeTypes() {
        for (BinaryPlanNode.Type type : BinaryPlanNode.Type.values()) {
            BinaryPlanNode node = new BinaryPlanNode(1, type);
            assertEquals(type, node.getType());
            assertEquals(type.name(), node.getExplainName());
        }
    }

    public void testBinaryPlanNodeTypes() {
        assertEquals(4, BinaryPlanNode.Type.values().length);
        assertNotNull(BinaryPlanNode.Type.AS_PERCENT);
        assertNotNull(BinaryPlanNode.Type.DIFF);
        assertNotNull(BinaryPlanNode.Type.DIVIDE_SERIES);
        assertNotNull(BinaryPlanNode.Type.FALLBACK_SERIES);
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(BinaryPlanNode planNode) {
            return "visit BinaryPlanNode";
        }
    }
}
