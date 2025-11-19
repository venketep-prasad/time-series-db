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

import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for HistogramPercentilePlanNode.
 */
public class HistogramPercentilePlanNodeTests extends BasePlanNodeTests {

    public void testHistogramPercentilePlanNodeCreation() {
        List<Float> percentiles = Arrays.asList(50.0f, 95.0f, 99.0f);
        HistogramPercentilePlanNode node = new HistogramPercentilePlanNode(1, "bucket_id", "bucket_range", percentiles);

        assertEquals(1, node.getId());
        assertEquals("bucket_id", node.getBucketId());
        assertEquals("bucket_range", node.getBucketRange());
        assertEquals(percentiles, node.getPercentiles());
        assertEquals("HISTOGRAM_PERCENTILE(bucket_id, bucket_range, [50.0, 95.0, 99.0])", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testHistogramPercentilePlanNodeVisitorAccept() {
        HistogramPercentilePlanNode node = new HistogramPercentilePlanNode(1, "le", "bucket", List.of(90.0f));
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit HistogramPercentilePlanNode", result);
    }

    public void testHistogramPercentilePlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("histogramPercentile");
        functionNode.addChildNode(new ValueNode("bucket_id"));
        functionNode.addChildNode(new ValueNode("bucket_range"));
        functionNode.addChildNode(new ValueNode("50.0"));
        functionNode.addChildNode(new ValueNode("95.0"));

        HistogramPercentilePlanNode node = HistogramPercentilePlanNode.of(functionNode);

        assertEquals("bucket_id", node.getBucketId());
        assertEquals("bucket_range", node.getBucketRange());
        assertEquals(Arrays.asList(50.0f, 95.0f), node.getPercentiles());
    }

    public void testHistogramPercentilePlanNodeFactoryMethodSinglePercentile() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("histogramPercentile");
        functionNode.addChildNode(new ValueNode("le"));
        functionNode.addChildNode(new ValueNode("bucket"));
        functionNode.addChildNode(new ValueNode("99.9"));

        HistogramPercentilePlanNode node = HistogramPercentilePlanNode.of(functionNode);

        assertEquals("le", node.getBucketId());
        assertEquals("bucket", node.getBucketRange());
        assertEquals(List.of(99.9f), node.getPercentiles());
    }

    public void testHistogramPercentilePlanNodeGetPercentilesReturnsDefensiveCopy() {
        List<Float> originalPercentiles = Arrays.asList(50.0f, 95.0f);
        HistogramPercentilePlanNode node = new HistogramPercentilePlanNode(1, "id", "range", originalPercentiles);

        List<Float> returnedPercentiles = node.getPercentiles();
        returnedPercentiles.add(99.0f);

        assertEquals(2, node.getPercentiles().size());
        assertFalse(node.getPercentiles().contains(99.0f));
    }

    public void testHistogramPercentilePlanNodeFactoryMethodThrowsOnInsufficientArgs() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("histogramPercentile");
        functionNode.addChildNode(new ValueNode("bucket_id"));
        functionNode.addChildNode(new ValueNode("bucket_range"));

        expectThrows(IllegalArgumentException.class, () -> HistogramPercentilePlanNode.of(functionNode));
    }

    public void testHistogramPercentilePlanNodeFactoryMethodThrowsOnInvalidPercentile() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("histogramPercentile");
        functionNode.addChildNode(new ValueNode("bucket_id"));
        functionNode.addChildNode(new ValueNode("bucket_range"));
        functionNode.addChildNode(new ValueNode("150.0"));

        expectThrows(IllegalArgumentException.class, () -> HistogramPercentilePlanNode.of(functionNode));
    }

    public void testHistogramPercentilePlanNodeFactoryMethodAcceptsZeroPercentile() {
        // Test that 0.0 is a valid percentile (boundary case - minimum value)
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("histogramPercentile");
        functionNode.addChildNode(new ValueNode("bucket_id"));
        functionNode.addChildNode(new ValueNode("bucket_range"));
        functionNode.addChildNode(new ValueNode("0.0"));

        HistogramPercentilePlanNode node = HistogramPercentilePlanNode.of(functionNode);

        assertEquals("bucket_id", node.getBucketId());
        assertEquals("bucket_range", node.getBucketRange());
        assertEquals(List.of(0.0f), node.getPercentiles());
    }

    public void testHistogramPercentilePlanNodeFactoryMethodThrowsOnNegativePercentile() {
        // Test that negative percentiles are rejected
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("histogramPercentile");
        functionNode.addChildNode(new ValueNode("bucket_id"));
        functionNode.addChildNode(new ValueNode("bucket_range"));
        functionNode.addChildNode(new ValueNode("-1.0"));

        expectThrows(IllegalArgumentException.class, () -> HistogramPercentilePlanNode.of(functionNode));
    }

    public void testHistogramPercentilePlanNodeFactoryMethodAccepts100Percentile() {
        // Test that 100.0 is a valid percentile (boundary case)
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("histogramPercentile");
        functionNode.addChildNode(new ValueNode("bucket_id"));
        functionNode.addChildNode(new ValueNode("bucket_range"));
        functionNode.addChildNode(new ValueNode("100.0"));

        HistogramPercentilePlanNode node = HistogramPercentilePlanNode.of(functionNode);

        assertEquals("bucket_id", node.getBucketId());
        assertEquals("bucket_range", node.getBucketRange());
        assertEquals(List.of(100.0f), node.getPercentiles());
    }

    public void testHistogramPercentilePlanNodeFactoryMethodThrowsOnNonNumericPercentile() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("histogramPercentile");
        functionNode.addChildNode(new ValueNode("bucket_id"));
        functionNode.addChildNode(new ValueNode("bucket_range"));
        functionNode.addChildNode(new ValueNode("not_a_number"));

        expectThrows(IllegalArgumentException.class, () -> HistogramPercentilePlanNode.of(functionNode));
    }

    public void testHistogramPercentilePlanNodeFactoryMethodThrowsOnNonValueNode() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("histogramPercentile");
        functionNode.addChildNode(new FunctionNode()); // non value node
        functionNode.addChildNode(new ValueNode("bucket_range"));
        functionNode.addChildNode(new ValueNode("50.0"));

        expectThrows(IllegalArgumentException.class, () -> HistogramPercentilePlanNode.of(functionNode));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(HistogramPercentilePlanNode planNode) {
            return "visit HistogramPercentilePlanNode";
        }
    }
}
