/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.integrationTests;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.tsdb.framework.RestTimeSeriesTestFramework;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Integration tests for compressed mode correctness.
 *
 * <p><b>Single-index test</b> ({@code compressed_mode_rest_it.yaml}):
 * Validates basic compressed mode with a single index — fetch, filter, nulls,
 * and pushdown vs no-pushdown parity for moving sum.</p>
 *
 * <p><b>Multi-shard test</b> ({@code multiple_shard_compression_rest_it.yaml}):
 * Validates compressed mode for multi-index coordinator merge, live series vs
 * closed chunk reads, and no-pushdown on all queries. The same series spans two
 * indices, forcing the coordinator to merge compressed data from different shards.</p>
 *
 * <p><b>Parity testing strategy:</b> The multi-shard YAML is executed twice —
 * once without compression (baseline) and once with compression enabled. Both runs
 * validate against identical expected values, proving compression doesn't affect
 * correctness.</p>
 *
 * <p>Both the versioned serialization setting and the compression setting are enabled
 * to match the real rollout procedure.</p>
 */
public class CompressedModeRestIT extends RestTimeSeriesTestFramework {

    private static final Logger logger = LogManager.getLogger(CompressedModeRestIT.class);
    private static final String COMPRESSED_MODE_REST_IT = "test_cases/compressed_mode_rest_it.yaml";
    private static final String MULTI_SHARD_COMPRESSION_REST_IT = "test_cases/multiple_shard_compression_rest_it.yaml";
    private static final String COMPRESSION_SETTING = "tsdb_engine.query.enable_internal_agg_chunk_compression";
    private static final String VERSIONED_SERIALIZATION_SETTING = "tsdb_engine.query.enable_versioned_serialization";

    private static final String[] MULTI_SHARD_INDICES = { "multi_shard_comp_test_1", "multi_shard_comp_test_2" };

    /**
     * Custom index settings with a reduced {@code ooo_cutoff} for the closed chunk test.
     *
     * <p>Chunk closure requires {@code chunk.maxTimestamp <= maxTime - oooCutoffWindow}.
     * With the default {@code ooo_cutoff: 1d} and test data spanning only ~1 hour,
     * no chunks are ever closeable. Using {@code ooo_cutoff: 5m} ensures that chunks
     * whose max timestamp is more than 5 minutes before the latest sample become
     * closeable on flush.</p>
     *
     * <p>With {@code chunk_duration: 20m} (default) and data spanning ~1 hour,
     * multiple 20-minute chunks are created. After flush, older chunks move to
     * ClosedChunkIndex while the most recent stays in LiveSeriesIndex,
     * creating a mixed closed+live scenario that exercises both leaf readers.</p>
     */
    private static final String CLOSED_CHUNK_INDEX_SETTINGS = """
        index.refresh_interval: "1s"
        index.tsdb_engine.enabled: true
        index.tsdb_engine.labels.storage_type: binary
        index.tsdb_engine.lang.m3.default_step_size: "10s"
        index.tsdb_engine.ooo_cutoff: "5m"
        index.queries.cache.enabled: false
        index.requests.cache.enable: false
        index.translog.durability: async
        index.translog.sync_interval: "1s"
        """;

    /**
     * When true, {@link #setupTest()} will flush all multi-shard test indices
     * after ingestion to move closeable chunks to ClosedChunkIndex.
     */
    private boolean flushAfterSetup = false;

    @Override
    protected void setupTest() throws IOException {
        super.setupTest();
        if (flushAfterSetup) {
            flushIndices(MULTI_SHARD_INDICES);
        }
    }

    // -------------------------------------------------------------------------
    // Single-index compressed mode test (original)
    // -------------------------------------------------------------------------

    /**
     * Tests compressed mode correctness with a single index.
     * Enables compression, runs queries with and without pushdown,
     * and validates results match expected values.
     */
    public void testCompressedModeCorrectness() throws Exception {
        try {
            enableClusterSetting(VERSIONED_SERIALIZATION_SETTING);
            enableClusterSetting(COMPRESSION_SETTING);

            initializeTest(COMPRESSED_MODE_REST_IT);
            runBasicTest();
        } finally {
            disableClusterSetting(COMPRESSION_SETTING);
            disableClusterSetting(VERSIONED_SERIALIZATION_SETTING);
        }
    }

    // -------------------------------------------------------------------------
    // Multi-shard compressed mode parity tests
    // -------------------------------------------------------------------------

    /**
     * Tests compressed mode parity with data in the <b>live series index</b>.
     *
     * <p>Uses the default {@code ooo_cutoff: 1d}. Since the test data spans only ~1 hour,
     * no chunks are closeable ({@code cutoff = maxTime - 1d}, well before any data).
     * The periodic flush task (every 10s) may fire but will not close any chunks.
     * All data remains in LiveSeriesIndex, exercising
     * {@code LiveSeriesIndexLeafReader.rawChunkDataForDoc()}.</p>
     */
    public void testMultiShardLiveSeriesParity() throws Exception {
        flushAfterSetup = false;

        // Phase 1: Without compression (baseline)
        logger.info("Phase 1: Running without compression (live series baseline)");
        initializeTest(MULTI_SHARD_COMPRESSION_REST_IT);
        runBasicTest();

        deleteIndices(MULTI_SHARD_INDICES);

        // Phase 2: With compression (parity proof)
        try {
            logger.info("Phase 2: Running with compression (live series compressed mode)");
            enableClusterSetting(VERSIONED_SERIALIZATION_SETTING);
            enableClusterSetting(COMPRESSION_SETTING);

            initializeTest(MULTI_SHARD_COMPRESSION_REST_IT);
            runBasicTest();
        } finally {
            disableClusterSetting(COMPRESSION_SETTING);
            disableClusterSetting(VERSIONED_SERIALIZATION_SETTING);
        }
    }

    /**
     * Tests compressed mode parity with data in the <b>closed chunk index</b>.
     *
     * <p>Uses custom index settings with {@code ooo_cutoff: 5m} so that older chunks
     * become closeable. After ingestion, an explicit flush moves eligible chunks from
     * LiveSeriesIndex to ClosedChunkIndex. With {@code chunk_duration: 20m} (default)
     * and data spanning ~1 hour, older chunks move to ClosedChunkIndex while the most
     * recent stays in LiveSeriesIndex — exercising both leaf readers in the same query.</p>
     */
    public void testMultiShardClosedChunkParity() throws Exception {
        flushAfterSetup = true;

        // Phase 1: Without compression (closed chunks baseline)
        logger.info("Phase 1: Running without compression (closed chunk baseline)");
        initializeTest(MULTI_SHARD_COMPRESSION_REST_IT, CLOSED_CHUNK_INDEX_SETTINGS);
        runBasicTest();

        deleteIndices(MULTI_SHARD_INDICES);

        // Phase 2: With compression (parity proof)
        try {
            logger.info("Phase 2: Running with compression (closed chunk compressed mode)");
            enableClusterSetting(VERSIONED_SERIALIZATION_SETTING);
            enableClusterSetting(COMPRESSION_SETTING);

            initializeTest(MULTI_SHARD_COMPRESSION_REST_IT, CLOSED_CHUNK_INDEX_SETTINGS);
            runBasicTest();
        } finally {
            disableClusterSetting(COMPRESSION_SETTING);
            disableClusterSetting(VERSIONED_SERIALIZATION_SETTING);
        }
    }

    // -------------------------------------------------------------------------
    // Helper methods
    // -------------------------------------------------------------------------

    private void flushIndices(String[] indices) throws IOException {
        for (String index : indices) {
            Request flushRequest = new Request("POST", "/" + index + "/_flush");
            flushRequest.addParameter("force", "true");
            flushRequest.addParameter("wait_if_ongoing", "true");
            Response response = client().performRequest(flushRequest);
            assertEquals(200, response.getStatusLine().getStatusCode());
            logger.info("Flushed index: {}", index);
        }
    }

    private void deleteIndices(String[] indices) throws IOException {
        for (String index : indices) {
            try {
                client().performRequest(new Request("DELETE", "/" + index));
                logger.info("Deleted index: {}", index);
            } catch (Exception e) {
                logger.warn("Failed to delete index during cleanup: {}", index, e);
            }
        }
    }

    private void enableClusterSetting(String setting) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
              "transient": {
                "%s": true
              }
            }
            """, setting));
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        verifyClusterSettingEnabled(setting);
        logger.info("Enabled cluster setting: {}", setting);
    }

    private void disableClusterSetting(String setting) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
              "transient": {
                "%s": null
              }
            }
            """, setting));
        try {
            client().performRequest(request);
            logger.info("Disabled cluster setting (cleanup): {}", setting);
        } catch (Exception e) {
            logger.warn("Failed to disable cluster setting during cleanup: {}", setting, e);
        }
    }

    private void verifyClusterSettingEnabled(String setting) throws IOException {
        Request request = new Request("GET", "/_cluster/settings");
        request.addParameter("include_defaults", "false");
        request.addParameter("flat_settings", "true");
        Response response = client().performRequest(request);

        @SuppressWarnings("unchecked")
        Map<String, Object> settings = entityAsMap(response);

        @SuppressWarnings("unchecked")
        Map<String, Object> transientSettings = (Map<String, Object>) settings.get("transient");

        if (transientSettings != null && transientSettings.containsKey(setting)) {
            Object value = transientSettings.get(setting);
            assertTrue("Setting should be enabled (true): " + setting, "true".equals(value.toString()) || Boolean.TRUE.equals(value));
        } else {
            fail("Setting not found in cluster settings after enabling: " + setting);
        }
    }
}
