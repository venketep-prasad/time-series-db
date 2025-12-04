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
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for ExcludeByTagPlanNode.
 */
public class ExcludeByTagPlanNodeTests extends BasePlanNodeTests {

    /**
     * Test basic creation with ID, tag name, and single pattern.
     */
    public void testExcludeByTagPlanNodeCreation() {
        List<String> patterns = Collections.singletonList("production");
        ExcludeByTagPlanNode node = new ExcludeByTagPlanNode(1, "env", patterns);

        assertEquals(1, node.getId());
        assertEquals("env", node.getTagName());
        assertEquals(patterns, node.getPatterns());
        assertEquals("EXCLUDE_BY_TAG(tag=env,patterns=production)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    /**
     * Test creation with single pattern.
     */
    public void testExcludeByTagPlanNodeWithSinglePattern() {
        List<String> patterns = Collections.singletonList("prod.*");
        ExcludeByTagPlanNode node = new ExcludeByTagPlanNode(1, "environment", patterns);

        assertEquals("environment", node.getTagName());
        assertEquals(patterns, node.getPatterns());
        assertEquals("EXCLUDE_BY_TAG(tag=environment,patterns=prod.*)", node.getExplainName());
    }

    /**
     * Test creation with multiple patterns.
     */
    public void testExcludeByTagPlanNodeWithMultiplePatterns() {
        List<String> patterns = Arrays.asList("production", "staging", "dev.*");
        ExcludeByTagPlanNode node = new ExcludeByTagPlanNode(1, "env", patterns);

        assertEquals("env", node.getTagName());
        assertEquals(patterns, node.getPatterns());
        assertEquals("EXCLUDE_BY_TAG(tag=env,patterns=production, staging, dev.*)", node.getExplainName());
    }

    /**
     * Test getTagName() returns correct value.
     */
    public void testGetTagName() {
        ExcludeByTagPlanNode node = new ExcludeByTagPlanNode(1, "region", List.of("us-east"));

        assertEquals("region", node.getTagName());
    }

    /**
     * Test getPatterns() returns correct list.
     */
    public void testGetPatterns() {
        List<String> patterns = Arrays.asList("pattern1", "pattern2", "pattern3");
        ExcludeByTagPlanNode node = new ExcludeByTagPlanNode(1, "tag", patterns);

        assertEquals(patterns, node.getPatterns());
        assertEquals(3, node.getPatterns().size());
    }

    /**
     * Test getExplainName() format with different tag and pattern combinations.
     */
    public void testGetExplainName() {
        // Single pattern
        ExcludeByTagPlanNode node1 = new ExcludeByTagPlanNode(1, "status", List.of("error"));
        assertEquals("EXCLUDE_BY_TAG(tag=status,patterns=error)", node1.getExplainName());

        // Multiple patterns
        ExcludeByTagPlanNode node2 = new ExcludeByTagPlanNode(2, "host", Arrays.asList("host1", "host2"));
        assertEquals("EXCLUDE_BY_TAG(tag=host,patterns=host1, host2)", node2.getExplainName());
    }

    /**
     * Test visitor accept() method with mock visitor.
     */
    public void testExcludeByTagPlanNodeVisitorAccept() {
        ExcludeByTagPlanNode node = new ExcludeByTagPlanNode(1, "env", List.of("production"));
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit ExcludeByTagPlanNode", result);
    }

    /**
     * Test factory method with valid tag and single pattern.
     */
    public void testExcludeByTagPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("excludeByTag");
        functionNode.addChildNode(new ValueNode("\"env\""));
        functionNode.addChildNode(new ValueNode("\"production\""));

        ExcludeByTagPlanNode node = ExcludeByTagPlanNode.of(functionNode);

        assertEquals("env", node.getTagName());
        assertEquals(Collections.singletonList("production"), node.getPatterns());
    }

    /**
     * Test factory method with tag and multiple patterns.
     */
    public void testExcludeByTagPlanNodeFactoryMethodMultiplePatterns() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("excludeByTag");
        functionNode.addChildNode(new ValueNode("\"env\""));
        functionNode.addChildNode(new ValueNode("\"production\""));
        functionNode.addChildNode(new ValueNode("\"staging\""));
        functionNode.addChildNode(new ValueNode("\"dev.*\""));

        ExcludeByTagPlanNode node = ExcludeByTagPlanNode.of(functionNode);

        assertEquals("env", node.getTagName());
        assertEquals(Arrays.asList("production", "staging", "dev.*"), node.getPatterns());
    }

    /**
     * Test factory method with quoted string values.
     */
    public void testExcludeByTagPlanNodeFactoryMethodWithQuotedValues() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("excludeByTag");
        functionNode.addChildNode(new ValueNode("\"environment\""));
        functionNode.addChildNode(new ValueNode("\"prod.*\""));
        functionNode.addChildNode(new ValueNode("\"staging.*\""));

        ExcludeByTagPlanNode node = ExcludeByTagPlanNode.of(functionNode);

        assertEquals("environment", node.getTagName());
        assertEquals(Arrays.asList("prod.*", "staging.*"), node.getPatterns());
    }

    /**
     * Test factory method throws exception when no children (less than 2 arguments).
     */
    public void testFactoryMethodThrowsOnNoChildren() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("excludeByTag");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ExcludeByTagPlanNode.of(functionNode));
        assertTrue(exception.getMessage().contains("must specify tag and pattern"));
    }

    /**
     * Test factory method throws exception when only tag provided (no patterns).
     */
    public void testFactoryMethodThrowsOnOnlyTag() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("excludeByTag");
        functionNode.addChildNode(new ValueNode("\"env\""));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ExcludeByTagPlanNode.of(functionNode));
        assertTrue(exception.getMessage().contains("must specify tag and pattern"));
    }

    /**
     * Test factory method throws exception when first argument is not a ValueNode.
     */
    public void testFactoryMethodThrowsOnNonValueNodeForTag() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("excludeByTag");
        functionNode.addChildNode(new FunctionNode()); // Wrong type
        functionNode.addChildNode(new ValueNode("\"pattern\""));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ExcludeByTagPlanNode.of(functionNode));
        assertTrue(exception.getMessage().contains("First argument must be a value representing tag"));
    }

    /**
     * Test factory method throws exception when pattern argument is not a ValueNode.
     */
    public void testFactoryMethodThrowsOnNonValueNodeForPattern() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("excludeByTag");
        functionNode.addChildNode(new ValueNode("\"env\""));
        functionNode.addChildNode(new FunctionNode()); // Wrong type

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ExcludeByTagPlanNode.of(functionNode));
        assertTrue(exception.getMessage().contains("patterns argument must be a value"));
    }

    /**
     * Test that getPatterns() returns defensive copy to prevent external modification.
     */
    public void testGetPatternsReturnsDefensiveCopy() {
        List<String> originalPatterns = Arrays.asList("pattern1", "pattern2");
        ExcludeByTagPlanNode node = new ExcludeByTagPlanNode(1, "tag", originalPatterns);

        List<String> returnedPatterns = node.getPatterns();
        returnedPatterns.add("newPattern");
        // If we can add, check it doesn't affect the node's internal state
        assertEquals(2, node.getPatterns().size());
        assertFalse(node.getPatterns().contains("newPattern"));
    }

    /**
     * Test children management - adding child to plan node.
     */
    public void testExcludeByTagPlanNodeChildrenManagement() {
        ExcludeByTagPlanNode node = new ExcludeByTagPlanNode(1, "env", List.of("prod"));
        M3PlanNode child = new ExcludeByTagPlanNode(2, "region", List.of("us-east"));

        node.addChild(child);
        assertEquals(1, node.getChildren().size());
        assertEquals(child, node.getChildren().getFirst());
    }

    /**
     * Test that factory method generates valid IDs.
     */
    public void testFactoryMethodGeneratesValidId() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("excludeByTag");
        functionNode.addChildNode(new ValueNode("\"env\""));
        functionNode.addChildNode(new ValueNode("\"prod\""));

        ExcludeByTagPlanNode node = ExcludeByTagPlanNode.of(functionNode);

        assertTrue(node.getId() >= 0);
    }

    /**
     * Test with regex pattern containing special characters.
     */
    public void testExcludeByTagPlanNodeWithRegexPatterns() {
        List<String> patterns = Arrays.asList(".*-prod", "^staging-.*$", "[a-z]+");
        ExcludeByTagPlanNode node = new ExcludeByTagPlanNode(1, "environment", patterns);

        assertEquals("environment", node.getTagName());
        assertEquals(patterns, node.getPatterns());
        assertEquals("EXCLUDE_BY_TAG(tag=environment,patterns=.*-prod, ^staging-.*$, [a-z]+)", node.getExplainName());
    }

    /**
     * Test visitor integration verifies correct type.
     */
    public void testVisitorReturnsCorrectType() {
        ExcludeByTagPlanNode node = new ExcludeByTagPlanNode(1, "tag", List.of("pattern"));
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);

        assertNotNull(result);
        assertEquals("visit ExcludeByTagPlanNode", result);
        assertNotEquals("process called", result); // Should call specific visit, not generic process
    }

    /**
     * Mock visitor for testing the visitor pattern.
     */
    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(ExcludeByTagPlanNode planNode) {
            return "visit ExcludeByTagPlanNode";
        }
    }
}
