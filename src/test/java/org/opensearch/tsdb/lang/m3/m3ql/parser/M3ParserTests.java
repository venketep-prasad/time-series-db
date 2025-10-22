/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.lang.m3.M3TestUtils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.generated.M3QLParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

public class M3ParserTests extends OpenSearchTestCase {

    private String testCaseName;
    private String query;
    private String expected;

    public M3ParserTests(TestCaseData testData) {
        this.testCaseName = testData.name;
        this.query = testData.query;
        this.expected = testData.expected;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        try {
            List<Object[]> testCases = new ArrayList<>();

            // Add AST generation test cases
            NavigableMap<String, String> queries = TestUtils.getResourceFilesWithExtension("lang/m3/data/queries", ".m3ql");
            NavigableMap<String, String> expectedResults = TestUtils.getResourceFilesWithExtension("lang/m3/data/ast", ".txt");

            if (queries.size() != expectedResults.size()) {
                throw new IllegalStateException("Number of query files does not match result files");
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
            M3TestUtils.printAST(M3QLParser.parse(query, true), 0, ps);
            assertEquals("AST does not match for test case: " + testCaseName, expected, baos.toString(StandardCharsets.UTF_8));
        } catch (Exception e) {
            fail("Failed to parse query for test case " + testCaseName + ": " + query + " with error: " + e.getMessage());
        }
    }

    public static class TestCaseData {
        public final String name;
        public final String query;
        public final String expected;

        public TestCaseData(String name, String query, String expected) {
            this.name = name;
            this.query = query;
            this.expected = expected;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
