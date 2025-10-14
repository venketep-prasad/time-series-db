/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.plugin.rest;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Utility class for REST API response validation in tests.
 *
 * <p>Provides common helper methods for testing REST endpoints, including:
 * <ul>
 *   <li>JSON response parsing</li>
 *   <li>Success response structure validation</li>
 *   <li>Error response structure validation</li>
 *   <li>Matrix response result extraction</li>
 * </ul>
 *
 * <p>This class is specifically for REST API response validation. For test data creation,
 * see {@link org.opensearch.tsdb.util.TestDataBuilder}.
 */
@SuppressWarnings("unchecked")
public final class RestTestUtils {

    private RestTestUtils() {
        // Utility class - no instantiation
    }

    // ========== Response Parsing Methods ==========

    /**
     * Parses a REST response content as JSON and returns it as a Map.
     *
     * @param responseContent the JSON response content
     * @return parsed response as a map
     * @throws IOException if parsing fails
     */
    public static Map<String, Object> parseJsonResponse(String responseContent) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, responseContent)
        ) {
            return parser.map();
        }
    }

    // ========== Response Validation Methods ==========

    /**
     * Validates the basic structure of a successful matrix response.
     *
     * <p>Checks that the response:
     * <ul>
     *   <li>Has HTTP 200 OK status</li>
     *   <li>Contains "status": "success"</li>
     *   <li>Contains "data" object with "resultType": "matrix"</li>
     *   <li>Contains "result" array</li>
     * </ul>
     *
     * @param response the REST response to validate
     * @return parsed response map for further assertions
     * @throws IOException if parsing fails
     */
    public static Map<String, Object> validateSuccessResponse(RestResponse response) throws IOException {
        assertEquals(RestStatus.OK, response.status());
        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());

        assertThat(parsed.get("status"), equalTo("success"));
        assertNotNull(parsed.get("data"));

        Map<String, Object> data = (Map<String, Object>) parsed.get("data");
        assertThat(data.get("resultType"), equalTo("matrix"));
        assertNotNull(data.get("result"));

        return parsed;
    }

    /**
     * Validates the structure of an error response.
     *
     * <p>Checks that the response:
     * <ul>
     *   <li>Has HTTP 500 Internal Server Error status</li>
     *   <li>Contains "status": "error"</li>
     *   <li>Contains "error" field with error message</li>
     * </ul>
     *
     * @param response the REST response to validate
     * @throws IOException if parsing fails
     */
    public static void validateErrorResponse(RestResponse response) throws IOException {
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response.status());
        Map<String, Object> parsed = parseJsonResponse(response.content().utf8ToString());

        assertThat(parsed.get("status"), equalTo("error"));
        assertNotNull(parsed.get("error"));
    }

    // ========== Result Extraction Methods ==========

    /**
     * Extracts the result array from a parsed matrix response.
     *
     * <p>Navigates the response structure to return the array at: data.result</p>
     *
     * @param parsed the parsed response map
     * @return list of result items from the matrix response
     */
    public static List<Map<String, Object>> getResultArray(Map<String, Object> parsed) {
        Map<String, Object> data = (Map<String, Object>) parsed.get("data");
        return (List<Map<String, Object>>) data.get("result");
    }
}
