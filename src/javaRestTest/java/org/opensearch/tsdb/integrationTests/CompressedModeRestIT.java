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
 * Integration tests verifying compressed mode produces identical results to traditional mode.
 * Tests run queries with pushdown=true and pushdown=false to validate correctness.
 *
 * <p>This test enables both the versioned serialization setting
 * ({@code tsdb_engine.query.enable_versioned_serialization}) and the compression setting
 * ({@code tsdb_engine.query.enable_internal_agg_chunk_compression}) to match the real rollout
 * procedure: versioned serialization must be enabled first so all nodes understand the new wire
 * format before compressed chunks are sent.</p>
 */
public class CompressedModeRestIT extends RestTimeSeriesTestFramework {

    private static final Logger logger = LogManager.getLogger(CompressedModeRestIT.class);
    private static final String COMPRESSED_MODE_REST_IT = "test_cases/compressed_mode_rest_it.yaml";
    private static final String COMPRESSION_SETTING = "tsdb_engine.query.enable_internal_agg_chunk_compression";
    private static final String VERSIONED_SERIALIZATION_SETTING = "tsdb_engine.query.enable_versioned_serialization";

    public void testCompressedModeCorrectness() throws Exception {
        try {
            // Enable versioned serialization first (must precede compression in rollout)
            enableClusterSetting(VERSIONED_SERIALIZATION_SETTING);
            verifyClusterSettingEnabled(VERSIONED_SERIALIZATION_SETTING);

            // Then enable compressed mode
            enableClusterSetting(COMPRESSION_SETTING);
            verifyClusterSettingEnabled(COMPRESSION_SETTING);

            // Run the test with both settings enabled
            initializeTest(COMPRESSED_MODE_REST_IT);
            runBasicTest();
        } finally {
            // Clean up: disable both settings after test (reverse order)
            disableClusterSetting(COMPRESSION_SETTING);
            disableClusterSetting(VERSIONED_SERIALIZATION_SETTING);
        }
    }

    /**
     * Enables a cluster setting by setting it to true.
     */
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
        logger.info("Enabled cluster setting: {}", setting);
    }

    /**
     * Disables a cluster setting by resetting it to null.
     */
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

    /**
     * Verifies that a cluster setting is enabled by querying cluster settings.
     */
    private void verifyClusterSettingEnabled(String setting) throws IOException {
        Request request = new Request("GET", "/_cluster/settings");
        request.addParameter("include_defaults", "false");
        request.addParameter("flat_settings", "true");
        Response response = client().performRequest(request);

        @SuppressWarnings("unchecked")
        Map<String, Object> settings = entityAsMap(response);

        // Check transient settings
        @SuppressWarnings("unchecked")
        Map<String, Object> transientSettings = (Map<String, Object>) settings.get("transient");

        if (transientSettings != null && transientSettings.containsKey(setting)) {
            Object value = transientSettings.get(setting);
            assertTrue("Setting should be enabled (true): " + setting, "true".equals(value.toString()) || Boolean.TRUE.equals(value));
            logger.info("Verified setting is enabled: {} = {}", setting, value);
        } else {
            fail("Setting not found in cluster settings after enabling: " + setting);
        }
    }
}
