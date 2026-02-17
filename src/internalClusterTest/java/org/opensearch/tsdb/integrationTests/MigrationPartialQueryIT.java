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
 * Integration test for migration scenarios where only PART of a query involves migrated data.
 *
 * <p>This test validates the split-fetch-stitch approach when a complex query involves both
 * migrated and non-migrated metrics, ensuring correct results when:
 * <ul>
 *   <li>One metric in a multi-metric query spans multiple indices (migrated)</li>
 *   <li>Other metrics in the same query reside in a single index (not migrated)</li>
 * </ul>
 *
 * <p><b>Problem Statement:</b>
 * Real-world migration scenarios often involve selective migration where:
 * <ul>
 *   <li>High-priority metrics are migrated to new infrastructure</li>
 *   <li>Low-priority or reference metrics remain in the original index</li>
 *   <li>Queries may combine both types of metrics (e.g., asPercent, sumSeries)</li>
 * </ul>
 *
 * <p><b>Test Scenarios:</b>
 *
 * <p><b>Scenario 1: asPercent with Partial Migration</b>
 * <pre>
 * Query: (fetch service:a name:success | moving 10m sum) | asPercent(fetch service:a name:total | moving 10m sum)
 * </pre>
 * <ul>
 *   <li><code>name:success</code> metric is MIGRATED (spans C1 [0m, 40m] and C2 [20m, 60m])</li>
 *   <li><code>name:total</code> metric is NOT migrated (only in C1 for full range [0m, 60m])</li>
 *   <li>The success metric must be split-fetch-stitched BEFORE computing asPercent with total</li>
 * </ul>
 *
 * <p><b>Scenario 2: sumSeries with Selective Migration</b>
 * <pre>
 * Query: fetch name:success | sumSeries name | moving 5m sum
 * </pre>
 * <ul>
 *   <li><code>service:a name:success</code> is MIGRATED (spans C1 and C2)</li>
 *   <li><code>service:b name:success</code> is NOT migrated (only in C1 for full range)</li>
 *   <li>The fetch must split-fetch-stitch for service:a while fetching service:b normally</li>
 *   <li>Results are combined via sumSeries before applying moving sum</li>
 * </ul>
 *
 * <p><b>Solution: Selective Split-Fetch-Stitch:</b>
 * <ol>
 *   <li>Identify which metrics/series require split-fetch-stitch (migrated ones)</li>
 *   <li>Apply split-fetch-stitch ONLY to migrated metrics</li>
 *   <li>Fetch non-migrated metrics normally from their single index</li>
 *   <li>Combine results at the coordinator level for multi-series operations</li>
 * </ol>
 *
 * <p><b>Timeline:</b>
 * <ul>
 *   <li>t1 = 0m - C1 starts</li>
 *   <li>t3 = 20m - C2 starts (migration begins)</li>
 *   <li>t2 = 40m - C1 migration completes (migrated metrics end in C1)</li>
 *   <li>t4 = 60m - C2 ends</li>
 *   <li>Overlap: [20m, 40m] - migrated metrics exist in BOTH indices</li>
 * </ul>
 *
 * <p><b>Test Verification:</b>
 * Each test case compares:
 * <ul>
 *   <li><b>Baseline:</b> Normal M3 query on a single index with all metrics (no migration)</li>
 *   <li><b>Migration:</b> DSL query with selective split-fetch-stitch on mixed indices</li>
 * </ul>
 * The framework validates that both approaches produce identical results.
 *
 * <p>Configuration is loaded from {@code test_cases/migration_partial_query_it.yaml}
 */
public class MigrationPartialQueryIT extends TimeSeriesTestFramework {

    private static final String TEST_YAML = "test_cases/migration_partial_query_it.yaml";
    private static final String SUMSERIES_TEST_YAML = "test_cases/migration_sumseries_it.yaml";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TSDBPlugin.class);
    }

    /**
     * Tests asPercent operation where only the numerator metric is migrated.
     *
     * <p>Query: {@code (fetch service:a name:success | moving 10m sum) | asPercent(fetch service:a name:total | moving 10m sum)}
     *
     * <p><b>Setup:</b>
     * <ul>
     *   <li>success metric: MIGRATED - C1 [0m, 40m], C2 [20m, 60m] with overlap</li>
     *   <li>total metric: NOT migrated - C1 [0m, 60m] full range</li>
     * </ul>
     *
     * <p><b>Expected Behavior:</b>
     * <ul>
     *   <li>Success metric undergoes split-fetch-stitch with moving sum applied selectively</li>
     *   <li>Total metric fetched normally with moving sum pushdown</li>
     *   <li>asPercent computed on coordinator: (stitched_success / total) * 100</li>
     *   <li>Result matches baseline query on non-migrated data</li>
     * </ul>
     */
    public void testAsPercentWithPartialMigration() throws Exception {
        loadTestConfigurationFromFile(TEST_YAML);
        runBasicTest();
        // If we reach here, both baseline and migration queries produced identical results!
    }

    /**
     * Tests sumSeries operation where only some series are migrated.
     *
     * <p>Query: {@code fetch name:success | sumSeries name | moving 5m sum}
     *
     * <p><b>Setup:</b>
     * <ul>
     *   <li>service:a name:success: MIGRATED - C1 [0m, 40m], C2 [20m, 60m]</li>
     *   <li>service:b name:success: NOT migrated - C1 [0m, 60m] full range</li>
     * </ul>
     *
     * <p><b>Expected Behavior:</b>
     * <ul>
     *   <li>Service:a undergoes split-fetch-stitch (3 fetches with selective lookback)</li>
     *   <li>Service:b fetched normally for full range</li>
     *   <li>sumSeries combines both services by grouping on 'name' label</li>
     *   <li>Moving 5m sum applied to the summed result</li>
     *   <li>Result matches baseline query on non-migrated data</li>
     * </ul>
     */
    public void testSumSeriesWithSelectiveMigration() throws Exception {
        loadTestConfigurationFromFile(SUMSERIES_TEST_YAML);
        runBasicTest();
        // If we reach here, both baseline and migration queries produced identical results!
    }
}
