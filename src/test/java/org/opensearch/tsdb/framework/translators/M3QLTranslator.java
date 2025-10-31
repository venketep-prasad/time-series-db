/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.translators;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tsdb.framework.models.QueryConfig;
import org.opensearch.tsdb.framework.models.TimeConfig;
import org.opensearch.tsdb.lang.m3.dsl.M3OSTranslator;

import java.time.Instant;

/**
 * Translator for M3QL queries.
 * Converts M3QL query strings to OpenSearch SearchRequest using the M3OSTranslator.
 * Used in both REST tests and internal cluster tests.
 */
public class M3QLTranslator implements QueryConfigTranslator {

    @Override
    public SearchRequest translate(QueryConfig queryConfig, String indexName) throws Exception {
        String queryString = queryConfig.query();
        TimeConfig config = queryConfig.config();

        if (config == null) {
            throw new IllegalArgumentException("Query config is required for M3QL query execution");
        }

        // Use already-parsed timestamps and duration from config
        Instant minTime = config.minTimestamp();
        Instant maxTime = config.maxTimestamp();
        long step = config.step().toMillis();

        // Get pushdown setting from query config (default is pushdown enabled, so disablePushdown=false means pushdown=true)
        boolean pushdown = !queryConfig.isDisablePushdown();

        // Create M3OSTranslator parameters
        M3OSTranslator.Params params = new M3OSTranslator.Params(minTime.toEpochMilli(), maxTime.toEpochMilli(), step, pushdown, false);

        // Translate M3QL query string to SearchSourceBuilder
        SearchSourceBuilder searchSource = M3OSTranslator.translate(queryString, params);

        // Create SearchRequest with the index name
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSource);

        return searchRequest;
    }
}
