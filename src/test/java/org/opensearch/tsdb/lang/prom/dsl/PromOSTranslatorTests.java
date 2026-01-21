/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.dsl;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.tsdb.lang.prom.promql.parser.PromParserTests;
import org.opensearch.tsdb.lang.prom.promql.plan.PromASTConverter;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

/**
 * Parameterized tests for PromQL to OpenSearch DSL translation.
 * Tests end-to-end translation from PromQL query strings to OpenSearch Query DSL.
 */
public class PromOSTranslatorTests extends OpenSearchTestCase {

    private static final long START_TIME = 1_000_000_000;
    private static final long END_TIME = START_TIME + 1_000_000;
    private static final long STEP = 100_000;

    private final String testCaseName;
    private final String query;
    private final String expected;

    public PromOSTranslatorTests(PromParserTests.TestCaseData testData) {
        this.testCaseName = testData.name();
        this.query = testData.query();
        this.expected = testData.expected();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        try {
            List<Object[]> testCases = new ArrayList<>();

            NavigableMap<String, String> queries = TestUtils.getResourceFilesWithExtension("lang/prom/data/queries", ".promql");
            NavigableMap<String, String> expectedDsl = TestUtils.getResourceFilesWithExtension("lang/prom/data/dsl", ".dsl");

            if (queries.size() != expectedDsl.size()) {
                throw new IllegalStateException("Number of query files does not match DSL result files");
            }

            Iterator<String> queryKeys = queries.keySet().iterator();
            Iterator<String> queryIterator = queries.values().iterator();
            Iterator<String> dslIterator = expectedDsl.values().iterator();

            while (queryIterator.hasNext()) {
                String testCaseName = queryKeys.next();
                String query = queryIterator.next();
                String dsl = dslIterator.next().trim(); // remove trailing newline
                testCases.add(new Object[] { new PromParserTests.TestCaseData(testCaseName, query, dsl) });
            }

            return testCases;
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException("Failed to load test data: " + e.getMessage(), e);
        }
    }

    public void testPromOSTranslator() {
        try {
            // Reset ID generator for consistent output
            PromASTConverter.resetIdGenerator();

            SearchSourceBuilder searchSourceBuilder = PromOSTranslator.translate(
                query,
                new PromOSTranslator.Params(Constants.Time.DEFAULT_TIME_UNIT, START_TIME, END_TIME, STEP, 0L, true, false)
            );

            // Use string comparison to ensure formatting remains consistent
            assertEquals("DSL does not match for test case: " + testCaseName, expected, prettyPrint(searchSourceBuilder.toString()));
        } catch (IOException e) {
            // Handle parsing errors (e.g., if one of the strings is not valid JSON)
            fail("Error parsing JSON string: " + e.getMessage() + " for test case: " + testCaseName);
        } catch (Exception e) {
            fail("Failed to run translator test for test case " + testCaseName + ": " + query + " with error: " + e.getMessage());
        }
    }

    private String prettyPrint(String json) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json
            )
        ) {
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                builder.prettyPrint();
                builder.copyCurrentStructure(parser);
                return builder.toString();
            }
        }
    }
}
