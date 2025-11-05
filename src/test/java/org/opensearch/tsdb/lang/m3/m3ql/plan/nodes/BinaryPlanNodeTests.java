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
public abstract class BinaryPlanNodeTests extends BasePlanNodeTests {

    private BinaryPlanNode node;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.node = getBinaryPlanNode();
    }

    protected abstract BinaryPlanNode getBinaryPlanNode();

    public void verifyPlanNodeName(String name) {
        assertEquals(name, node.getExplainName());
    }

    public void verifyVisitorAccept() {
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit BinaryPlanNode", result);
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
