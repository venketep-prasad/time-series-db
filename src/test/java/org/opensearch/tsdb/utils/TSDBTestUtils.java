/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.utils;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

public class TSDBTestUtils {

    public static String createSampleJson(Labels labels, long timestamp, double val) throws IOException {
        return createSampleJson(getLabelsString(labels), timestamp, val);
    }

    public static String createSampleJson(String labelsString, long timestamp, double val) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("labels", labelsString).field("timestamp", timestamp).field("value", val);
            builder.endObject();
            return builder.toString();
        }
    }

    /**
     * Formats labels as space-separated key-value pairs.
     * Example: "key1 value1 key2 value2"
     */
    private static String getLabelsString(Labels labels) {
        Map<String, String> labelsMap = labels.toMapView();
        if (labelsMap.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : labelsMap.entrySet()) {
            if (!first) {
                sb.append(" ");
            }
            first = false;
            sb.append(String.format(Locale.ROOT, "%s %s", entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }
}
