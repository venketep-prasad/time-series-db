/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.promql.parser;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.lang.prom.PromTestUtils;
import org.opensearch.tsdb.lang.prom.promql.parser.generated.PromQLParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

/**
 * Parameterized tests for PromQL parser.
 * Tests AST generation for various PromQL queries.
 */
public class PromParserTests extends OpenSearchTestCase {

    private final String testCaseName;
    private final String query;
    private final String expected;

    public PromParserTests(TestCaseData testData) {
        this.testCaseName = testData.name;
        this.query = testData.query;
        this.expected = testData.expected;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        try {
            List<Object[]> testCases = new ArrayList<>();

            // Load test queries and expected AST outputs
            NavigableMap<String, String> queries = TestUtils.getResourceFilesWithExtension("lang/prom/data/queries", ".promql");
            NavigableMap<String, String> expectedResults = TestUtils.getResourceFilesWithExtension("lang/prom/data/ast", ".txt");

            if (queries.size() != expectedResults.size()) {
                throw new IllegalStateException("Number of query files does not match AST result files");
            }

            Iterator<String> queryKeys = queries.keySet().iterator();
            Iterator<String> queryIterator = queries.values().iterator();
            Iterator<String> resultIterator = expectedResults.values().iterator();

            while (queryIterator.hasNext()) {
                String testCaseName = queryKeys.next();
                String query = queryIterator.next();
                String expected = resultIterator.next();
                testCases.add(new Object[] { new TestCaseData(testCaseName, query, expected) });
            }

            return testCases;
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException("Failed to load test data: " + e.getMessage(), e);
        }
    }

    public void testASTGeneration() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8);
            PromTestUtils.printAST(PromQLParser.parse(query), 0, ps);
            assertEquals("AST does not match for test case: " + testCaseName, expected, baos.toString(StandardCharsets.UTF_8));
        } catch (Exception e) {
            fail("Failed to parse query for test case " + testCaseName + ": " + query + " with error: " + e.getMessage());
        }
    }

    public record TestCaseData(String name, String query, String expected) {

        @Override
        public String toString() {
            return name;
        }
    }
}
