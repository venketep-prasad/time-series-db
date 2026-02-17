/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.integrationTests;

import org.opensearch.plugins.Plugin;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.framework.TimeSeriesTestFramework;

import java.util.Collection;
import java.util.List;

/**
 * Integration test for split fetch with stitch functionality.
 *
 * <p>This test validates that:
 * <ul>
 *   <li>Splitting a time range query into multiple sub-queries works correctly</li>
 *   <li>The StitchStage properly merges results from multiple sub-fetches</li>
 *   <li>The stitched result is identical to a normal non-split query</li>
 * </ul>
 *
 * <p>Test Scenario:
 * <ol>
 *   <li>Ingest time series data spanning 40 minutes (9 data points at 5-minute intervals)</li>
 *   <li>Execute a normal M3QL query that fetches and aggregates all data</li>
 *   <li>Execute a DSL query with split fetch + stitch:
 *     <ul>
 *       <li>R1: First 20 minutes (4 data points)</li>
 *       <li>R2: Next 15 minutes (3 data points)</li>
 *       <li>R3: Last 5 minutes (2 data points)</li>
 *       <li>StitchStage merges all sub-query results</li>
 *     </ul>
 *   </li>
 *   <li>Framework automatically compares both results - they must be identical</li>
 * </ol>
 *
 * <p>The YAML configuration (split_fetch_stitch_it.yaml) defines both queries
 * with identical expected results, ensuring they produce the same output.
 */
public class SplitFetchStitchIT extends TimeSeriesTestFramework {

    private static final String TEST_YAML = "test_cases/split_fetch_stitch_it.yaml";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TSDBPlugin.class);
    }

    /**
     * Main test that validates split fetch with stitch produces identical results to normal query.
     * The framework automatically executes both queries defined in the YAML and validates
     * that they produce identical results.
     */
    public void testSplitFetchWithStitchMatchesNormalQuery() throws Exception {
        // Load configuration, ingest data, and execute/validate both queries
        loadTestConfigurationFromFile(TEST_YAML);
        runBasicTest();

        // If we reach here, both queries executed successfully and produced identical results!
    }
}
