/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.translators;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.framework.models.QueryConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Translator for DSL queries.
 * Accepts a direct DSL body as a Map and converts it to OpenSearch SearchRequest.
 * This allows testing with hand-crafted DSL that includes split fetch + stitch aggregations.
 *
 * <p>Used for advanced testing scenarios where direct control over the query DSL is needed,
 * such as testing the stitch stage with multiple sub-fetch aggregations.
 */
public class DSLTranslator implements QueryConfigTranslator {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final NamedXContentRegistry xContentRegistry;

    public DSLTranslator() {
        // Create NamedXContentRegistry with TSDBPlugin registrations
        // This allows parsing of custom aggregations like time_series_unfold and ts_coordinator
        List<SearchPlugin> plugins = List.of(new TSDBPlugin());
        SearchModule searchModule = new SearchModule(Settings.EMPTY, plugins);
        this.xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    public SearchRequest translate(QueryConfig queryConfig, String indices) throws Exception {
        Map<String, Object> dslBody;

        // Load DSL from file if dsl_file is specified, otherwise use inline dsl_body
        if (queryConfig.dslFile() != null && !queryConfig.dslFile().isEmpty()) {
            dslBody = loadDslFromFile(queryConfig.dslFile());
        } else {
            dslBody = queryConfig.dslBody();
        }

        if (dslBody == null || dslBody.isEmpty()) {
            throw new IllegalArgumentException("Either dsl_body or dsl_file is required for DSL query type");
        }

        // Convert the Map to SearchSourceBuilder via JSON
        SearchSourceBuilder searchSource = mapToSearchSourceBuilder(dslBody);

        // Create SearchRequest with parsed indices (supports CCS patterns)
        String[] indexArray = parseIndices(indices);
        SearchRequest searchRequest = new SearchRequest(indexArray);
        searchRequest.source(searchSource);

        // Apply CCS minimize roundtrips setting if this is a cross-cluster query
        if (queryConfig.isCrossClusterQuery()) {
            searchRequest.setCcsMinimizeRoundtrips(queryConfig.isCcsMinimizeRoundtrips());
        }

        return searchRequest;
    }

    /**
     * Convert a Map representation of DSL to SearchSourceBuilder.
     *
     * @param dslMap The DSL as a Map
     * @return SearchSourceBuilder parsed from the Map
     * @throws IOException if parsing fails
     */
    private SearchSourceBuilder mapToSearchSourceBuilder(Map<String, Object> dslMap) throws IOException {
        // Convert Map to JSON string
        String jsonString = OBJECT_MAPPER.writeValueAsString(dslMap);

        // Parse JSON string to SearchSourceBuilder using OpenSearch's XContent parser
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                xContentRegistry, // Use TSDB plugin registry for custom aggregations
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                jsonString
            )
        ) {
            return SearchSourceBuilder.fromXContent(parser);
        }
    }

    /**
     * Load DSL from a JSON file in the classpath.
     *
     * @param filePath Path to the JSON file (relative to resources)
     * @return Map representation of the DSL
     * @throws IOException if file reading or parsing fails
     */
    private Map<String, Object> loadDslFromFile(String filePath) throws IOException {
        try (var inputStream = getClass().getClassLoader().getResourceAsStream(filePath)) {
            if (inputStream == null) {
                throw new IllegalArgumentException("DSL file not found: " + filePath);
            }
            return OBJECT_MAPPER.readValue(inputStream, Map.class);
        }
    }
}
