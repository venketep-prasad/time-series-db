/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.lang.m3.stage.AbstractGroupingSampleStage;

/**
 * Configuration for parallel processing in grouping sample stages (sum, avg, min, max, count, etc.)
 * at coordinator level.
 *
 * <p>This configuration is specifically for {@link AbstractGroupingSampleStage}
 * and its subclasses. When pushdown is disabled, these stages execute on the coordinator node.
 * For large datasets, parallel processing can improve performance by utilizing multiple CPU cores.</p>
 *
 * <h2>Applicable Stages:</h2>
 * <ul>
 *   <li>SumStage - parallel summation across time series</li>
 *   <li>AvgStage - parallel averaging with SumCountSample</li>
 *   <li>MinStage, MaxStage - parallel min/max computation</li>
 *   <li>CountStage - parallel counting</li>
 *   <li>Other stages extending AbstractGroupingSampleStage</li>
 * </ul>
 *
 * <h2>Thread Safety:</h2>
 * <p>Parallel processing uses thread-local aggregation, then merges partial
 * results, avoiding lock contention. Work runs on {@link java.util.concurrent.ForkJoinPool}
 * common pool.</p>
 *
 * <h2>Settings:</h2>
 * <p>Settings are defined in {@link TSDBPlugin}:</p>
 * <ul>
 *   <li>{@link TSDBPlugin#GROUPING_STAGE_PARALLEL_ENABLED}</li>
 *   <li>{@link TSDBPlugin#GROUPING_STAGE_PARALLEL_TOTAL_WORK_THRESHOLD}</li>
 * </ul>
 *
 * <h2>Performance:</h2>
 * <p>JMH benchmarks show parallel processing wins at all tested data points (series x samples &gt;= 10,000),
 * with speedups ranging from 1.2x at 10K total work to 7-8x at 1M+ total work. The total work threshold
 * ({@code seriesCount * avgSamplesPerSeries}) is used instead of separate series/samples thresholds because
 * the actual cost is proportional to the product, not to each dimension independently.</p>
 *
 * @param enabled whether parallel processing is enabled
 * @param totalWorkThreshold minimum total work (series x samples) for parallel processing
 * @see AbstractGroupingSampleStage
 * @see TSDBPlugin
 */
public record ParallelProcessingConfig(boolean enabled, long totalWorkThreshold) {

    private static final Logger logger = LogManager.getLogger(ParallelProcessingConfig.class);

    /**
     * Initialize parallel processing configuration from cluster settings and register dynamic update listeners.
     * Called from TSDBPlugin.createComponents() once per node startup.
     *
     * <p>This method:</p>
     * <ol>
     *   <li>Creates a ParallelProcessingConfig from current settings</li>
     *   <li>Sets it on AbstractGroupingSampleStage</li>
     *   <li>Registers listeners for dynamic setting updates</li>
     * </ol>
     *
     * @param clusterSettings the cluster settings for registering dynamic listeners
     * @param settings the current node settings
     */
    public static void initialize(ClusterSettings clusterSettings, Settings settings) {
        // Initialize with current settings
        ParallelProcessingConfig initialConfig = new ParallelProcessingConfig(
            TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED.get(settings),
            TSDBPlugin.GROUPING_STAGE_PARALLEL_TOTAL_WORK_THRESHOLD.get(settings)
        );
        AbstractGroupingSampleStage.setParallelConfig(initialConfig);
        logger.info(
            "Initialized parallel processing config: enabled={}, totalWorkThreshold={}",
            initialConfig.enabled(),
            initialConfig.totalWorkThreshold()
        );

        // Register listeners for each setting
        clusterSettings.addSettingsUpdateConsumer(TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED, ParallelProcessingConfig::updateEnabled);
        clusterSettings.addSettingsUpdateConsumer(
            TSDBPlugin.GROUPING_STAGE_PARALLEL_TOTAL_WORK_THRESHOLD,
            ParallelProcessingConfig::updateTotalWorkThreshold
        );
    }

    /**
     * Update the enabled setting while preserving other values.
     * Package-private for testing.
     */
    static void updateEnabled(boolean newEnabled) {
        ParallelProcessingConfig current = AbstractGroupingSampleStage.getParallelConfig();
        ParallelProcessingConfig newConfig = new ParallelProcessingConfig(newEnabled, current.totalWorkThreshold());
        AbstractGroupingSampleStage.setParallelConfig(newConfig);
        logger.info("Updated parallel processing config: enabled={}", newEnabled);
    }

    /**
     * Update the total work threshold setting while preserving other values.
     * Package-private for testing.
     */
    static void updateTotalWorkThreshold(long newThreshold) {
        ParallelProcessingConfig current = AbstractGroupingSampleStage.getParallelConfig();
        ParallelProcessingConfig newConfig = new ParallelProcessingConfig(current.enabled(), newThreshold);
        AbstractGroupingSampleStage.setParallelConfig(newConfig);
        logger.info("Updated parallel processing config: totalWorkThreshold={}", newThreshold);
    }

    /**
     * Determine if parallel processing should be used for the given dataset in a grouping stage.
     * Uses total work (series x samples) as the decision criterion, since the actual computational
     * cost is proportional to the product of series count and samples per series.
     *
     * @param seriesCount number of time series to process
     * @param avgSamplesPerSeries average number of samples per series
     * @return true if parallel processing should be used
     */
    public boolean shouldUseParallelProcessing(int seriesCount, int avgSamplesPerSeries) {
        if (!enabled || seriesCount == 0) {
            return false;
        }

        long totalWork = (long) seriesCount * avgSamplesPerSeries;
        return totalWork >= totalWorkThreshold;
    }

    /**
     * Default configuration for when settings are not available.
     * Uses a conservative total work threshold determined by JMH benchmarks showing
     * parallel processing wins at all tested data points >= 10,000 total work.
     *
     * @return default configuration
     */
    public static ParallelProcessingConfig defaultConfig() {
        return new ParallelProcessingConfig(true, 10_000L);
    }

    /**
     * Configuration that always uses sequential processing.
     * Useful for testing or when parallel processing should be disabled.
     *
     * @return sequential-only configuration
     */
    public static ParallelProcessingConfig sequentialOnly() {
        return new ParallelProcessingConfig(false, Long.MAX_VALUE);
    }

    /**
     * Configuration that always uses parallel processing.
     * Useful for testing parallel code paths.
     *
     * @return always-parallel configuration
     */
    public static ParallelProcessingConfig alwaysParallel() {
        return new ParallelProcessingConfig(true, 0L);
    }
}
