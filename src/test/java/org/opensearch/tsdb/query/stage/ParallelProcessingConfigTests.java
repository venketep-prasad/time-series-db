/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.lang.m3.stage.AbstractGroupingSampleStage;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;

/**
 * Tests for ParallelProcessingConfig.
 */
public class ParallelProcessingConfigTests extends OpenSearchTestCase {

    /**
     * Test default configuration values.
     */
    public void testDefaultConfig() {
        ParallelProcessingConfig config = ParallelProcessingConfig.defaultConfig();

        assertFalse("Default config should be disabled (matches setting default)", config.enabled());
        assertEquals("Default total work threshold should be 10000", 10_000L, config.totalWorkThreshold());
    }

    /**
     * Test sequential-only configuration.
     */
    public void testSequentialOnlyConfig() {
        ParallelProcessingConfig config = ParallelProcessingConfig.sequentialOnly();

        assertFalse("Sequential-only config should be disabled", config.enabled());
        assertFalse("Disabled config should not use parallel", config.shouldUseParallelProcessing(Long.MAX_VALUE));
    }

    /**
     * Test always-parallel configuration.
     */
    public void testAlwaysParallelConfig() {
        ParallelProcessingConfig config = ParallelProcessingConfig.alwaysParallel();

        assertTrue("Always-parallel config should be enabled", config.enabled());
        assertEquals("Total work threshold should be 0", 0L, config.totalWorkThreshold());
        assertTrue("Should use parallel even with minimal data", config.shouldUseParallelProcessing(1));
    }

    /**
     * Test threshold logic - totalSamples must meet threshold.
     */
    public void testThresholdLogic() {
        ParallelProcessingConfig config = new ParallelProcessingConfig(true, 10_000L);

        // Below threshold
        assertFalse("Should not use parallel when below threshold", config.shouldUseParallelProcessing(5_000));

        // At threshold
        assertTrue("Should use parallel at exactly threshold", config.shouldUseParallelProcessing(10_000));

        // Above threshold
        assertTrue("Should use parallel when above threshold", config.shouldUseParallelProcessing(20_000));

        // Edge case: zero
        assertFalse(config.shouldUseParallelProcessing(0));

        // Large values
        assertTrue(config.shouldUseParallelProcessing(Long.MAX_VALUE));
    }

    /**
     * Test setting default values when using empty settings.
     */
    public void testSettingDefaultsMatchDefaultConfig() {
        Settings emptySettings = Settings.EMPTY;

        assertEquals(
            TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED.getDefault(emptySettings),
            TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED.get(emptySettings)
        );
        assertEquals(
            (long) TSDBPlugin.GROUPING_STAGE_PARALLEL_TOTAL_WORK_THRESHOLD.getDefault(emptySettings),
            (long) TSDBPlugin.GROUPING_STAGE_PARALLEL_TOTAL_WORK_THRESHOLD.get(emptySettings)
        );
    }

    /**
     * Test setting definitions have correct properties.
     */
    public void testSettingProperties() {
        assertTrue("Enabled setting should be dynamic", TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED.isDynamic());
        assertTrue("Total work threshold setting should be dynamic", TSDBPlugin.GROUPING_STAGE_PARALLEL_TOTAL_WORK_THRESHOLD.isDynamic());

        assertEquals(Boolean.FALSE, TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED.getDefault(Settings.EMPTY));
        assertEquals(Long.valueOf(10_000L), TSDBPlugin.GROUPING_STAGE_PARALLEL_TOTAL_WORK_THRESHOLD.getDefault(Settings.EMPTY));
    }

    /**
     * Test initialize method and dynamic setting updates via ClusterSettings.applySettings().
     */
    public void testInitializeAndDynamicUpdates() {
        // Create initial settings
        Settings initialSettings = Settings.builder()
            .put("tsdb_engine.query.grouping_stage.parallel_processing.enabled", true)
            .put("tsdb_engine.query.grouping_stage.parallel_processing.total_work_threshold", 10000)
            .build();

        // Create ClusterSettings with all our settings registered
        Set<org.opensearch.common.settings.Setting<?>> settingsSet = new HashSet<>();
        settingsSet.add(TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED);
        settingsSet.add(TSDBPlugin.GROUPING_STAGE_PARALLEL_TOTAL_WORK_THRESHOLD);
        settingsSet.add(TSDBPlugin.GROUPING_STAGE_PARALLEL_POOL_SIZE);

        ClusterSettings clusterSettings = new ClusterSettings(initialSettings, settingsSet);

        // Initialize - this registers the listeners
        ParallelProcessingConfig.initialize(clusterSettings, initialSettings);

        // Verify initial state
        ParallelProcessingConfig config = AbstractGroupingSampleStage.getParallelConfig();
        assertTrue("Initial: enabled should be true", config.enabled());
        assertEquals("Initial: total work threshold should be 10000", 10_000L, config.totalWorkThreshold());

        // Dynamically update enabled to false
        clusterSettings.applySettings(
            Settings.builder()
                .put("tsdb_engine.query.grouping_stage.parallel_processing.enabled", false)
                .put("tsdb_engine.query.grouping_stage.parallel_processing.total_work_threshold", 10000)
                .build()
        );
        config = AbstractGroupingSampleStage.getParallelConfig();
        assertFalse("After update: enabled should be false", config.enabled());

        // Dynamically update total work threshold
        clusterSettings.applySettings(
            Settings.builder()
                .put("tsdb_engine.query.grouping_stage.parallel_processing.enabled", false)
                .put("tsdb_engine.query.grouping_stage.parallel_processing.total_work_threshold", 50000)
                .build()
        );
        config = AbstractGroupingSampleStage.getParallelConfig();
        assertEquals("After update: total work threshold should be 50000", 50_000L, config.totalWorkThreshold());

        // Re-enable parallel processing
        clusterSettings.applySettings(
            Settings.builder()
                .put("tsdb_engine.query.grouping_stage.parallel_processing.enabled", true)
                .put("tsdb_engine.query.grouping_stage.parallel_processing.total_work_threshold", 50000)
                .build()
        );
        config = AbstractGroupingSampleStage.getParallelConfig();
        assertTrue("After re-enable: enabled should be true", config.enabled());

        // Reset
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.defaultConfig());
        ParallelProcessingConfig.shutdown();
    }

    /**
     * Test that getPool() returns a non-null pool after initialization.
     */
    public void testGetPoolReturnsNonNull() {
        // Before initialization, should fall back to commonPool
        ParallelProcessingConfig.shutdown();
        ForkJoinPool pool = ParallelProcessingConfig.getPool();
        assertNotNull("getPool() should never return null", pool);
        assertEquals("Should fall back to commonPool", ForkJoinPool.commonPool(), pool);
    }

    /**
     * Test that getPool() returns the dedicated pool after initialization.
     */
    public void testGetPoolReturnsDedicatedPoolAfterInit() {
        Settings settings = Settings.builder()
            .put("tsdb_engine.query.grouping_stage.parallel_processing.enabled", true)
            .put("tsdb_engine.query.grouping_stage.parallel_processing.total_work_threshold", 10000)
            .put("tsdb_engine.query.grouping_stage.parallel_processing.pool_size", 2)
            .build();

        Set<org.opensearch.common.settings.Setting<?>> settingsSet = new HashSet<>();
        settingsSet.add(TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED);
        settingsSet.add(TSDBPlugin.GROUPING_STAGE_PARALLEL_TOTAL_WORK_THRESHOLD);
        settingsSet.add(TSDBPlugin.GROUPING_STAGE_PARALLEL_POOL_SIZE);

        ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        ParallelProcessingConfig.initialize(clusterSettings, settings);

        ForkJoinPool pool = ParallelProcessingConfig.getPool();
        assertNotNull(pool);
        assertNotSame("Should not be commonPool", ForkJoinPool.commonPool(), pool);
        assertEquals("Pool parallelism should be 2", 2, pool.getParallelism());

        ParallelProcessingConfig.shutdown();
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.defaultConfig());
    }

    /**
     * Test that pool size setting has correct properties.
     */
    public void testPoolSizeSettingProperties() {
        assertTrue("Pool size setting should be dynamic", TSDBPlugin.GROUPING_STAGE_PARALLEL_POOL_SIZE.isDynamic());
        int defaultPoolSize = TSDBPlugin.GROUPING_STAGE_PARALLEL_POOL_SIZE.getDefault(Settings.EMPTY);
        assertTrue("Default pool size should be >= 1", defaultPoolSize >= 1);
        assertTrue("Default pool size should be <= available processors", defaultPoolSize <= Runtime.getRuntime().availableProcessors());
    }

    /**
     * Test that shutdown is idempotent (calling twice is safe).
     */
    public void testShutdownIdempotent() {
        ParallelProcessingConfig.shutdown();
        ParallelProcessingConfig.shutdown(); // should not throw

        // After shutdown, getPool falls back to commonPool
        assertEquals(ForkJoinPool.commonPool(), ParallelProcessingConfig.getPool());
    }
}
