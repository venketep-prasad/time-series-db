/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.xcontent.smile.SmileXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;

public class TSDBDocumentTests extends OpenSearchTestCase {

    public void testFromParsedDocumentWithAllFields() throws IOException {
        // Create a parsed document with all fields
        String labelsString = "env prod region us-west host server01";
        long timestamp = 1234567890000L;
        double value = 42.5;
        long reference = 999L;

        BytesReference source = createJsonSource(labelsString, timestamp, value, reference);
        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc1", null, null, source, XContentType.JSON, null);

        TSDBDocument metricDoc = TSDBDocument.fromParsedDocument(parsedDoc);

        assertNotNull(metricDoc);
        assertNotNull(metricDoc.labels());
        assertEquals(timestamp, metricDoc.timestamp());
        assertEquals(value, metricDoc.value(), 0.001);
        assertEquals(Long.valueOf(reference), metricDoc.seriesReference());
        assertEquals(labelsString, metricDoc.rawLabelsString());

        // Verify labels are parsed correctly
        Labels labels = metricDoc.labels();
        assertEquals("prod", labels.get("env"));
        assertEquals("us-west", labels.get("region"));
        assertEquals("server01", labels.get("host"));
    }

    public void testFromParsedDocumentWithoutReference() throws IOException {
        String labelsString = "service api method GET";
        long timestamp = 9876543210000L;
        double value = 123.456;

        BytesReference source = createJsonSourceWithoutReference(labelsString, timestamp, value);
        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc2", null, null, source, XContentType.JSON, null);

        TSDBDocument metricDoc = TSDBDocument.fromParsedDocument(parsedDoc);

        assertNotNull(metricDoc);
        assertNotNull(metricDoc.labels());
        assertEquals(timestamp, metricDoc.timestamp());
        assertEquals(value, metricDoc.value(), 0.001);
        assertNull(metricDoc.seriesReference());
        assertEquals(labelsString, metricDoc.rawLabelsString());
    }

    public void testFromParsedDocumentWithoutLabels() throws IOException {
        long timestamp = 1111111111111L;
        double value = 99.99;
        long reference = 777L;

        BytesReference source = createJsonSourceWithoutLabels(timestamp, value, reference);
        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc3", null, null, source, XContentType.JSON, null);

        TSDBDocument metricDoc = TSDBDocument.fromParsedDocument(parsedDoc);

        assertNotNull(metricDoc);
        assertNull(metricDoc.labels());
        assertEquals(timestamp, metricDoc.timestamp());
        assertEquals(value, metricDoc.value(), 0.001);
        assertEquals(Long.valueOf(reference), metricDoc.seriesReference());
        assertNull(metricDoc.rawLabelsString());
    }

    public void testFromParsedDocumentMissingTimestamp() throws IOException {
        BytesReference source;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.Mapping.SAMPLE_VALUE, 42.0);
            builder.endObject();
            source = BytesReference.bytes(builder);
        }

        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc5", null, null, source, XContentType.JSON, null);

        RuntimeException exception = expectThrows(RuntimeException.class, () -> TSDBDocument.fromParsedDocument(parsedDoc));
        assertNotNull(exception);
    }

    public void testFromParsedDocumentMissingValue() throws IOException {
        BytesReference source;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, 1234567890000L);
            builder.endObject();
            source = BytesReference.bytes(builder);
        }

        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc6", null, null, source, XContentType.JSON, null);

        RuntimeException exception = expectThrows(RuntimeException.class, () -> TSDBDocument.fromParsedDocument(parsedDoc));
        assertNotNull(exception);
    }

    public void testFromParsedDocumentNull() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TSDBDocument.fromParsedDocument(null));
        assertEquals("ParsedDocument cannot be null", exception.getMessage());
    }

    public void testFromParsedDocumentNullSource() {
        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc7", null, null, null, XContentType.JSON, null);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TSDBDocument.fromParsedDocument(parsedDoc));
        assertEquals("ParsedDocument source cannot be null", exception.getMessage());
    }

    public void testFromParsedDocumentInvalidTimestampType() throws IOException {
        BytesReference source;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, "not-a-number");
            builder.field(Constants.Mapping.SAMPLE_VALUE, 42.0);
            builder.endObject();
            source = BytesReference.bytes(builder);
        }

        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc8", null, null, source, XContentType.JSON, null);

        RuntimeException exception = expectThrows(RuntimeException.class, () -> TSDBDocument.fromParsedDocument(parsedDoc));
        assertNotNull(exception);
    }

    public void testFromParsedDocumentInvalidValueType() throws IOException {
        BytesReference source;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, 1234567890000L);
            builder.field(Constants.Mapping.SAMPLE_VALUE, "not-a-number");
            builder.endObject();
            source = BytesReference.bytes(builder);
        }

        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc9", null, null, source, XContentType.JSON, null);

        RuntimeException exception = expectThrows(RuntimeException.class, () -> TSDBDocument.fromParsedDocument(parsedDoc));
        assertNotNull(exception);
    }

    public void testFromParsedDocumentWithEmptyLabels() throws IOException {
        long timestamp = 3333333333333L;
        double value = 77.77;
        long reference = 111L;

        BytesReference source = createJsonSource("", timestamp, value, reference);
        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc10", null, null, source, XContentType.JSON, null);

        TSDBDocument metricDoc = TSDBDocument.fromParsedDocument(parsedDoc);

        assertNotNull(metricDoc);
        assertNull(metricDoc.labels()); // Empty string should result in null labels
        assertEquals(timestamp, metricDoc.timestamp());
        assertEquals(value, metricDoc.value(), 0.001);
        assertEquals(Long.valueOf(reference), metricDoc.seriesReference());
        assertEquals("", metricDoc.rawLabelsString());
    }

    /**
     * Test with SMILE content type.
     */
    public void testFromParsedDocumentWithSmileContentType() throws IOException {
        String labelsString = "app metrics type counter";
        long timestamp = 6666666666666L;
        double value = 100.0;
        long reference = 888L;

        BytesReference source = createSmileSource(labelsString, timestamp, value, reference);
        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc12", null, null, source, XContentType.SMILE, null);

        TSDBDocument metricDoc = TSDBDocument.fromParsedDocument(parsedDoc);

        assertNotNull(metricDoc);
        assertNotNull(metricDoc.labels());
        assertEquals(timestamp, metricDoc.timestamp());
        assertEquals(value, metricDoc.value(), 0.001);
        assertEquals(Long.valueOf(reference), metricDoc.seriesReference());
        assertEquals(labelsString, metricDoc.rawLabelsString());

        // Verify labels
        Labels labels = metricDoc.labels();
        assertEquals("metrics", labels.get("app"));
        assertEquals("counter", labels.get("type"));
    }

    public void testFromParsedDocumentWithLongReference() throws IOException {
        String labelsString = "service api";
        long timestamp = 1234567890000L;
        double value = 42.0;
        long reference = 9223372036854775807L; // large number to test "long" type reference

        BytesReference source = createJsonSource(labelsString, timestamp, value, reference);
        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc13", null, null, source, XContentType.JSON, null);

        TSDBDocument metricDoc = TSDBDocument.fromParsedDocument(parsedDoc);

        assertNotNull(metricDoc);
        assertEquals(Long.valueOf(reference), metricDoc.seriesReference());
    }

    public void testFromParsedDocumentWithNullReference() throws IOException {
        String labelsString = "service api";
        long timestamp = 1234567890000L;
        double value = 42.0;

        BytesReference source;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.IndexSchema.LABELS, labelsString);
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, value);
            builder.nullField(Constants.IndexSchema.REFERENCE);
            builder.endObject();
            source = BytesReference.bytes(builder);
        }

        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc15", null, null, source, XContentType.JSON, null);

        TSDBDocument metricDoc = TSDBDocument.fromParsedDocument(parsedDoc);

        assertNotNull(metricDoc);
        assertNull(metricDoc.seriesReference());
    }

    public void testFromParsedDocumentWithInvalidReferenceType() throws IOException {
        String labelsString = "service api";
        long timestamp = 1234567890000L;
        double value = 42.0;

        BytesReference source;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.IndexSchema.LABELS, labelsString);
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, value);
            builder.field(Constants.IndexSchema.REFERENCE, 123.45); // Double, should fail
            builder.endObject();
            source = BytesReference.bytes(builder);
        }

        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc16", null, null, source, XContentType.JSON, null);

        RuntimeException exception = expectThrows(RuntimeException.class, () -> TSDBDocument.fromParsedDocument(parsedDoc));
        assertNotNull(exception);
    }

    public void testFromParsedDocumentWithInvalidLabelsType() throws IOException {
        long timestamp = 1234567890000L;
        double value = 42.0;

        BytesReference source;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.IndexSchema.LABELS, 12345); // Number instead of string, should fail
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, value);
            builder.endObject();
            source = BytesReference.bytes(builder);
        }

        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc17", null, null, source, XContentType.JSON, null);

        RuntimeException exception = expectThrows(RuntimeException.class, () -> TSDBDocument.fromParsedDocument(parsedDoc));
        assertNotNull(exception);
    }

    public void testFromParsedDocumentWithNullLabels() throws IOException {
        long timestamp = 1234567890000L;
        double value = 42.0;
        long reference = 999L;

        BytesReference source;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.nullField(Constants.IndexSchema.LABELS);
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, value);
            builder.field(Constants.IndexSchema.REFERENCE, reference);
            builder.endObject();
            source = BytesReference.bytes(builder);
        }

        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc18", null, null, source, XContentType.JSON, null);

        TSDBDocument metricDoc = TSDBDocument.fromParsedDocument(parsedDoc);

        assertNotNull(metricDoc);
        assertNull(metricDoc.labels());
        assertNull(metricDoc.rawLabelsString());
    }

    public void testFromParsedDocumentWithPositiveInfinity() throws IOException {
        String labelsString = "service api";
        long timestamp = 1234567890000L;

        BytesReference source;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.IndexSchema.LABELS, labelsString);
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, "+Inf");
            builder.endObject();
            source = BytesReference.bytes(builder);
        }

        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc19", null, null, source, XContentType.JSON, null);
        TSDBDocument metricDoc = TSDBDocument.fromParsedDocument(parsedDoc);

        assertNotNull(metricDoc);
        assertEquals(timestamp, metricDoc.timestamp());
        assertEquals(Double.POSITIVE_INFINITY, metricDoc.value(), 0.0);
    }

    public void testFromParsedDocumentWithNegativeInfinity() throws IOException {
        String labelsString = "service api";
        long timestamp = 1234567890000L;

        BytesReference source;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.IndexSchema.LABELS, labelsString);
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, "-Inf");
            builder.endObject();
            source = BytesReference.bytes(builder);
        }

        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc20", null, null, source, XContentType.JSON, null);
        TSDBDocument metricDoc = TSDBDocument.fromParsedDocument(parsedDoc);

        assertNotNull(metricDoc);
        assertEquals(timestamp, metricDoc.timestamp());
        assertEquals(Double.NEGATIVE_INFINITY, metricDoc.value(), 0.0);
    }

    public void testFromParsedDocumentWithNumericStringValue() throws IOException {
        String labelsString = "service api";
        long timestamp = 1234567890000L;

        BytesReference source;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.IndexSchema.LABELS, labelsString);
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, "42.5");
            builder.endObject();
            source = BytesReference.bytes(builder);
        }

        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc21", null, null, source, XContentType.JSON, null);

        TSDBDocument metricDoc = TSDBDocument.fromParsedDocument(parsedDoc);

        assertNotNull(metricDoc);
        assertEquals(timestamp, metricDoc.timestamp());
        assertEquals(42.5, metricDoc.value(), 0.001);
    }

    public void testFromParsedDocumentWithInvalidStringValue() throws IOException {
        String labelsString = "service api";
        long timestamp = 1234567890000L;

        BytesReference source;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.IndexSchema.LABELS, labelsString);
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, "not-a-number");
            builder.endObject();
            source = BytesReference.bytes(builder);
        }

        ParsedDocument parsedDoc = new ParsedDocument(null, null, "doc22", null, null, source, XContentType.JSON, null);
        RuntimeException exception = expectThrows(RuntimeException.class, () -> TSDBDocument.fromParsedDocument(parsedDoc));
        assertNotNull(exception);
    }

    // Helper methods to create test data
    private BytesReference createJsonSource(String labelsString, long timestamp, double value, long reference) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            if (labelsString != null) {
                builder.field(Constants.IndexSchema.LABELS, labelsString);
            }
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, value);
            builder.field(Constants.IndexSchema.REFERENCE, reference);
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    private BytesReference createJsonSourceWithoutReference(String labelsString, long timestamp, double value) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            if (labelsString != null) {
                builder.field(Constants.IndexSchema.LABELS, labelsString);
            }
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, value);
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    private BytesReference createJsonSourceWithoutLabels(long timestamp, double value, long reference) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, value);
            builder.field(Constants.IndexSchema.REFERENCE, reference);
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    private BytesReference createSmileSource(String labelsString, long timestamp, double value, long reference) throws IOException {
        try (XContentBuilder builder = SmileXContent.contentBuilder()) {
            builder.startObject();
            if (labelsString != null) {
                builder.field(Constants.IndexSchema.LABELS, labelsString);
            }
            builder.field(Constants.Mapping.SAMPLE_TIMESTAMP, timestamp);
            builder.field(Constants.Mapping.SAMPLE_VALUE, value);
            builder.field(Constants.IndexSchema.REFERENCE, reference);
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }
}
