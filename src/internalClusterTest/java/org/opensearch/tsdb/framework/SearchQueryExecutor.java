/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.transport.client.Client;
import org.opensearch.tsdb.framework.models.QueryConfig;
import org.opensearch.tsdb.framework.translators.QueryConfigTranslator;
import org.opensearch.tsdb.query.utils.AggregationNameExtractor;
import org.opensearch.tsdb.query.utils.TimeSeriesOutputMapper;
import org.opensearch.tsdb.query.utils.TimeSeriesOutputMapper.TimeSeriesResult;

import java.util.List;

/**
 * Executes queries using internal cluster search client and validates responses against expected Prometheus matrix format.
 * This executor is used in internal cluster tests where we have direct access to the OpenSearch client.
 */
public class SearchQueryExecutor extends BaseQueryExecutor {

    private final Client client;

    public SearchQueryExecutor(Client client) {
        if (client == null) {
            throw new IllegalArgumentException("Client cannot be null");
        }
        this.client = client;
    }

    /**
     * Execute a query and return Prometheus matrix response.
     *
     * @param query The query configuration containing query string and time range
     * @param indexName The target index to query
     * @return PromMatrixResponse containing query results in Prometheus format
     * @throws Exception if query execution fails
     */
    public PromMatrixResponse executeQuery(QueryConfig query, String indexName) throws Exception {
        QueryConfigTranslator translator = query.type().createTranslator();
        SearchRequest searchRequest = translator.translate(query, indexName);

        // Execute search using the provided client
        String finalAggName = AggregationNameExtractor.getFinalAggregationName(searchRequest.source());
        SearchResponse response = client.search(searchRequest).actionGet();

        // Extract Prometheus matrix from the response
        return extractPromMatrixFromResponse(response, finalAggName);
    }

    /**
     * Extract Prometheus matrix from search response.
     *
     * @param resp The OpenSearch search response
     * @param finalAggName The name of the final aggregation to extract
     * @return PromMatrixResponse containing the extracted time series data
     */
    private PromMatrixResponse extractPromMatrixFromResponse(SearchResponse resp, String finalAggName) {
        // Check if search failed or had shard failures
        if (resp.getFailedShards() > 0 || resp.status().getStatus() >= 400) {
            return new PromMatrixResponse(STATUS_FAILURE, new PromMatrixData(List.of()));
        }

        List<TimeSeriesResult> results = TimeSeriesOutputMapper.extractAndTransformToTimeSeriesResult(resp.getAggregations(), finalAggName);
        PromMatrixData data = new PromMatrixData(results);
        return new PromMatrixResponse(STATUS_SUCCESS, data);
    }

}
