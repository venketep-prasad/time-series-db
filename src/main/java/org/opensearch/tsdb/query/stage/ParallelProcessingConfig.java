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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Configuration for parallel processing in grouping sample stages (sum, avg, min, max, count, etc.)
 * at coordinator level.
 *
 * <p>This configuration is specifically for {@link AbstractGroupingSampleStage}
 * and its subclasses. When pushdown is disabled, these stages execute on the coordinator node.
 * For large datasets, parallel processing can improve performance by utilizing multiple CPU cores.</p>
 *
 * <h2>Thread Pool:</h2>
 * <p>Parallel processing uses a dedicated {@link ForkJoinPool} instead of the JVM-wide
 * {@code ForkJoinPool.commonPool()}. This prevents TSDB parallel queries from competing with
 * OpenSearch core and other plugins for common pool threads. The pool size is configurable
 * via {@link TSDBPlugin#GROUPING_STAGE_PARALLEL_POOL_SIZE} and defaults to half the available
 * processors (minimum 1). The pool uses daemon threads so it does not prevent JVM shutdown.</p>
 *
 * <h2>Thread Safety:</h2>
 * <p>The pool reference is managed via {@link AtomicReference} for safe concurrent access during
 * dynamic pool size updates. Parallel processing itself uses thread-local aggregation, then merges
 * partial results, avoiding lock contention.</p>
 *
 * <h2>Settings:</h2>
 * <p>Settings are defined in {@link TSDBPlugin}:</p>
 * <ul>
 *   <li>{@link TSDBPlugin#GROUPING_STAGE_PARALLEL_ENABLED}</li>
 *   <li>{@link TSDBPlugin#GROUPING_STAGE_PARALLEL_TOTAL_WORK_THRESHOLD}</li>
 *   <li>{@link TSDBPlugin#GROUPING_STAGE_PARALLEL_POOL_SIZE}</li>
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

    /** Timeout in seconds to wait for pool termination during shutdown. */
    private static final int POOL_SHUTDOWN_TIMEOUT_SECONDS = 30;

    /**
     * Dedicated ForkJoinPool for TSDB parallel grouping stages, managed via AtomicReference
     * for safe concurrent access during dynamic pool size updates.
     */
    private static final AtomicReference<ForkJoinPool> dedicatedPool = new AtomicReference<>();

    /**
     * Get the dedicated ForkJoinPool for parallel processing.
     * Falls back to commonPool() if not initialized (e.g., in unit tests).
     *
     * @return the dedicated pool, or commonPool() as fallback
     */
    public static ForkJoinPool getPool() {
        ForkJoinPool pool = dedicatedPool.get();
        return pool != null ? pool : ForkJoinPool.commonPool();
    }

    /**
     * Initialize parallel processing configuration from cluster settings and register dynamic update listeners.
     * Called from TSDBPlugin.createComponents() once per node startup.
     *
     * @param clusterSettings the cluster settings for registering dynamic listeners
     * @param settings the current node settings
     */
    public static void initialize(ClusterSettings clusterSettings, Settings settings) {
        // Create the dedicated ForkJoinPool
        int poolSize = TSDBPlugin.GROUPING_STAGE_PARALLEL_POOL_SIZE.get(settings);
        dedicatedPool.set(createPool(poolSize));

        // Initialize with current settings
        ParallelProcessingConfig initialConfig = new ParallelProcessingConfig(
            TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED.get(settings),
            TSDBPlugin.GROUPING_STAGE_PARALLEL_TOTAL_WORK_THRESHOLD.get(settings)
        );
        AbstractGroupingSampleStage.setParallelConfig(initialConfig);
        logger.info(
            "Initialized parallel processing config: enabled={}, totalWorkThreshold={}, poolSize={}",
            initialConfig.enabled(),
            initialConfig.totalWorkThreshold(),
            poolSize
        );

        // Register a single compound listener for enabled + threshold to ensure atomic updates
        clusterSettings.addSettingsUpdateConsumer(
            TSDBPlugin.GROUPING_STAGE_PARALLEL_ENABLED,
            TSDBPlugin.GROUPING_STAGE_PARALLEL_TOTAL_WORK_THRESHOLD,
            ParallelProcessingConfig::updateConfig
        );

        // Pool size updates require recreating the pool
        clusterSettings.addSettingsUpdateConsumer(TSDBPlugin.GROUPING_STAGE_PARALLEL_POOL_SIZE, ParallelProcessingConfig::updatePoolSize);
    }

    /**
     * Atomically update both settings at once, avoiding race conditions between separate listeners.
     * Package-private for testing.
     */
    static void updateConfig(boolean newEnabled, long newTotalWorkThreshold) {
        ParallelProcessingConfig newConfig = new ParallelProcessingConfig(newEnabled, newTotalWorkThreshold);
        AbstractGroupingSampleStage.setParallelConfig(newConfig);
        logger.info("Updated parallel processing config: enabled={}, totalWorkThreshold={}", newEnabled, newTotalWorkThreshold);
    }

    /**
     * Update the pool size by atomically swapping to a new pool and gracefully shutting down the old one.
     * Uses {@link AtomicReference#getAndSet} so concurrent callers of {@link #getPool()} always get
     * a valid (either old or new) pool — never null. The old pool completes running tasks before
     * terminating (graceful shutdown).
     * Package-private for testing.
     */
    static void updatePoolSize(int newPoolSize) {
        ForkJoinPool oldPool = dedicatedPool.getAndSet(createPool(newPoolSize));
        if (oldPool != null) {
            oldPool.shutdown(); // graceful: completes running tasks, rejects new submissions
        }
        logger.info("Updated parallel processing pool size: {}", newPoolSize);
    }

    /**
     * Create a new ForkJoinPool with daemon threads named for easy identification in thread dumps.
     */
    private static ForkJoinPool createPool(int parallelism) {
        return new ForkJoinPool(parallelism, pool -> {
            ForkJoinPool.ForkJoinWorkerThreadFactory defaultFactory = ForkJoinPool.defaultForkJoinWorkerThreadFactory;
            var thread = defaultFactory.newThread(pool);
            thread.setDaemon(true);
            thread.setName("tsdb-parallel-grouping-" + thread.getPoolIndex());
            return thread;
        },
            null, // use default uncaught exception handler
            false // FIFO mode = false (LIFO for better cache locality)
        );
    }

    /**
     * Shut down the dedicated pool and wait for running tasks to complete.
     * Called from TSDBPlugin.close() during node shutdown or plugin reload.
     */
    public static void shutdown() {
        ForkJoinPool pool = dedicatedPool.getAndSet(null);
        if (pool != null) {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(POOL_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    logger.warn("Parallel processing pool did not terminate within {}s, forcing shutdown", POOL_SHUTDOWN_TIMEOUT_SECONDS);
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("Shut down parallel processing pool");
        }
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
