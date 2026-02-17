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
 * Integration test for migration scenarios with lookback operations.
 *
 * <p>This test validates the split-fetch-stitch approach when a time series
 * spans multiple indices due to data migration with overlapping time ranges.
 *
 * <p><b>Problem Statement:</b>
 * In TSDB, there's an assumption that a single series should reside in a single shard.
 * However, during migration, a metric can span 2 different shards/indices with overlapping
 * time ranges. For example:
 * <ul>
 *   <li>Cluster C1 (index1) contains data for [t1, t2]</li>
 *   <li>Cluster C2 (index2) contains data for [t3, t4] where t3 &lt; t2 (overlap)</li>
 * </ul>
 *
 * <p><b>The Challenge with Lookback Operations:</b>
 * Operations like "movingSum 15m" or "keepLastValue 10m" require lookback data.
 * Simply pushing down these operations to shards during migration gives incorrect results
 * because the first window in C2 won't have enough historical data within that shard alone.
 *
 * <p><b>Solution: Split-Fetch-Stitch with Selective Pushdown:</b>
 * <ol>
 *   <li><b>Fetch1:</b> Pre-migration period [t1-lookback, t3] → [t1, t3] stitch
 *       <ul><li>Pushdown is SAFE (all data in C1)</li></ul>
 *   </li>
 *   <li><b>Fetch2:</b> Overlap period [t3, t2+lookback] → [t3, t2+lookback] stitch
 *       <ul><li>NO pushdown (data split between C1 and C2, compute on coordinator)</li></ul>
 *   </li>
 *   <li><b>Fetch3:</b> Post-migration period [t2, t4] → [t2+lookback, t4] stitch
 *       <ul><li>Pushdown is SAFE (all data in C2)</li></ul>
 *   </li>
 *   <li><b>Stitch:</b> Merge results from all three fetches</li>
 * </ol>
 *
 * <p><b>Test Verification:</b>
 * Each test case compares two approaches:
 * <ul>
 *   <li><b>Baseline:</b> Normal M3 query on a single index with complete data (no migration)</li>
 *   <li><b>Migration:</b> DSL query using split-fetch-stitch on two overlapping indices</li>
 * </ul>
 * The framework validates that both approaches produce identical results, proving correctness.
 *
 * <p><b>Test Scenarios:</b>
 * <ul>
 *   <li>Moving sum with 15-minute lookback</li>
 *   <li>KeepLastValue with 10-minute lookback</li>
 * </ul>
 *
 * <p>Configuration is loaded from {@code test_cases/migration_lookback_it.yaml}
 */
public class MigrationLookbackIT extends TimeSeriesTestFramework {

    private static final String TEST_YAML = "test_cases/migration_lookback_it.yaml";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TSDBPlugin.class);
    }

    /**
     * Tests that moving sum with lookback produces identical results for:
     * - Normal query on complete data (baseline)
     * - Split-fetch-stitch on migrated data spanning two indices
     *
     * <p>Query: {@code fetch __name__:cpu_usage | movingSum 15m}
     * <p>Lookback: 15 minutes (3 intervals at 5-minute step)
     */
    public void testMovingSumWithMigration() throws Exception {
        loadTestConfigurationFromFile(TEST_YAML);
        runBasicTest();
        // If we reach here, both baseline and migration queries produced identical results!
    }

    /**
     * Tests that keepLastValue with lookback produces identical results for:
     * - Normal query on complete data (baseline)
     * - Split-fetch-stitch on migrated data spanning two indices
     *
     * <p>Query: {@code fetch __name__:memory_usage | keepLastValue 10m}
     * <p>Lookback: 10 minutes (2 intervals at 5-minute step)
     * <p>Data includes intentional gaps (nulls) to verify fill behavior
     */
    public void testKeepLastValueWithMigration() throws Exception {
        loadTestConfigurationFromFile(TEST_YAML);
        runBasicTest();
        // If we reach here, both baseline and migration queries produced identical results!
    }
}
