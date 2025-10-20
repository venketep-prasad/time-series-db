/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.opensearch.common.Nullable;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;

/**
 * Represents a metric document containing one time series sample. It holds labels, timestamp and value for the
 * time series sample along with optional series reference. This class is also responsible for parsing the incoming
 * indexing payload into a TSDBDocument by extracting the required fields for tsdb.
 *
 * <p> Sample values support infinity values represented as string constants "+Inf" and "-Inf". For finite samples values,
 * a double type value or its string type equivalent must be provided.
 *
 * @param labels space separated key-value pair string identifying the time series, can be null if seriesReference is provided.
 * @param timestamp the timestamp of the sample in milliseconds since epoch
 * @param value the numeric value of the sample
 * @param seriesReference optional stable hash reference for the series
 * @param rawLabelsString the original space-separated labels string from the source document
 */
public record TSDBDocument(@Nullable Labels labels, long timestamp, double value, @Nullable Long seriesReference,
    @Nullable String rawLabelsString) {

    /**
     * The character separator for label key-value pairs within the labels string
     */
    private static final String LABELS_SEPARATOR = " ";

    /**
     * String constant representing positive infinity
     */
    private static final String POSITIVE_INFINITY_STRING = "+Inf";

    /**
     * String constant representing negative infinity
     */
    private static final String NEGATIVE_INFINITY_STRING = "-Inf";

    private static final ConstructingObjectParser<TSDBDocument, Void> PARSER = new ConstructingObjectParser<>(
        "tsdb_document_parser",
        args -> {
            String rawLabelsString = (String) args[0];
            Labels labels = null;
            if (rawLabelsString != null && rawLabelsString.isEmpty() == false) {
                labels = ByteLabels.fromStrings(rawLabelsString.split(LABELS_SEPARATOR));
            }
            long timestamp = (Long) args[1];
            double value = (Double) args[2];
            Long seriesReference = (Long) args[3];
            if (labels == null && seriesReference == null) {
                throw new RuntimeException("Either labels or seriesReference must be provided");
            }
            return new TSDBDocument(labels, timestamp, value, seriesReference, rawLabelsString);
        }
    );

    static {
        // args[0] - labels (optional)
        PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField(Constants.IndexSchema.LABELS));
        // args[1] - timestamp (required)
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), new ParseField(Constants.Mapping.SAMPLE_TIMESTAMP));
        // args[2] - value (required, supports double type and string +Inf/-Inf)
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> parseValue(p),
            new ParseField(Constants.Mapping.SAMPLE_VALUE),
            org.opensearch.core.xcontent.ObjectParser.ValueType.VALUE
        );
        // args[3] - seriesReference (optional)
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> parseSeriesReference(p),
            new ParseField(Constants.IndexSchema.REFERENCE),
            org.opensearch.core.xcontent.ObjectParser.ValueType.VALUE
        );
    }

    /**
     * Parse sample value. Supports:
     * <ul>
     *   <li>Numeric values (double, float, int, long)</li>
     *   <li>String "+Inf" for positive infinity</li>
     *   <li>String "-Inf" for negative infinity</li>
     *   <li>Numeric strings that can be parsed as doubles</li>
     * </ul>
     *
     * @param parser the XContentParser
     * @return the parsed double value (may be infinity)
     * @throws IOException if parsing fails
     */
    private static Double parseValue(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();

        if (token == XContentParser.Token.VALUE_STRING) {
            String stringValue = parser.text();
            if (POSITIVE_INFINITY_STRING.equals(stringValue)) {
                return Double.POSITIVE_INFINITY;
            } else if (NEGATIVE_INFINITY_STRING.equals(stringValue)) {
                return Double.NEGATIVE_INFINITY;
            } else {
                // Try to parse as a numeric string
                try {
                    return Double.parseDouble(stringValue);
                } catch (NumberFormatException e) {
                    throw new RuntimeException(
                        "value must be a number or '"
                            + POSITIVE_INFINITY_STRING
                            + "'/'"
                            + NEGATIVE_INFINITY_STRING
                            + "', got: "
                            + stringValue
                    );
                }
            }
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            return parser.doubleValue();
        } else {
            throw new RuntimeException("value must be a number or string, got token: " + token);
        }
    }

    /**
     * Parse series reference. Only accepts Long or Integer types, or null.
     * Throws RuntimeException for other types (e.g., Double, String).
     */
    private static Long parseSeriesReference(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        if (token == XContentParser.Token.VALUE_NUMBER) {
            XContentParser.NumberType numberType = parser.numberType();
            if (numberType == XContentParser.NumberType.LONG) {
                return parser.longValue();
            } else if (numberType == XContentParser.NumberType.INT) {
                return (long) parser.intValue();
            } else {
                throw new RuntimeException("seriesReference must be a long or integer value, got: " + numberType);
            }
        }
        throw new RuntimeException("seriesReference must be a long or integer value, got token: " + token);
    }

    /**
     * Create a TSDBDocument from given OpenSearch ParsedDocument by extracting required fields.
     * The ParsedDocument source payload is used to build the TSDBDocument. The following fields are expected:
     * <ol>
     *     <li> labels (a string containing space separated key value pairs)
     *     <li> timestamp
     *     <li> value
     *     <li> reference
     * </ol>
     *
     * @param parsedDoc The parsed document containing source payload
     * @return an instance of TSDBDocument
     */
    public static TSDBDocument fromParsedDocument(ParsedDocument parsedDoc) {
        if (parsedDoc == null) {
            throw new IllegalArgumentException("ParsedDocument cannot be null");
        }

        // Get the raw source from ParsedDocument
        BytesReference source = parsedDoc.source();
        if (source == null) {
            throw new IllegalArgumentException("ParsedDocument source cannot be null");
        }

        try {
            try (
                XContentParser parser = XContentHelper.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    source,
                    parsedDoc.getMediaType()
                )
            ) {
                return PARSER.apply(parser, null);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract TSDBDocument from source", e);
        }
    }
}
