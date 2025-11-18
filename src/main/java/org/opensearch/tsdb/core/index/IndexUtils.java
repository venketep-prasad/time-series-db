/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.TSDBDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility constants and methods for indexing time series data.
 *
 * This class provides common field names and utilities used across the indexing
 * system for time series data, including field names for labels, references,
 * chunks, and timestamps.
 */
public final class IndexUtils {

    /**
     * IndexUtils should not be instantiated
     */
    private IndexUtils() {
        // Utility class
    }

    /**
     * Decode labels from SortedSetDocValues into Labels object for both live series and closed chunk index.
     *
     * @param docId the document ID to extract labels for
     * @param tsdbDocValues the tsdb doc values containing labels data
     * @return the decoded labels for the document
     * @throws IOException if an error occurs reading the labels
     */
    public static Labels labelsForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException {
        SortedSetDocValues labelsDocValues = tsdbDocValues.getLabelsDocValues();
        if (labelsDocValues == null || !labelsDocValues.advanceExact(docId)) {
            return ByteLabels.emptyLabels();
        }

        int valueCount = labelsDocValues.docValueCount();
        // docValueCount() is equivalent to one plus the maximum ordinal, that means ordinal
        // range is [0, docValueCount() - 1]
        List<String> labelStrings = new ArrayList<>(valueCount);

        for (int i = 0; i < valueCount; i++) {
            long ord = labelsDocValues.nextOrd();

            BytesRef term = labelsDocValues.lookupOrd(ord);
            String labelKVString = term.utf8ToString();

            // Parse "key:value" format (labels are stored with colon separator)
            labelStrings.add(labelKVString);
        }

        // Convert to ByteLabels using the fromStrings method
        return ByteLabels.fromSortedKeyValuePairs(labelStrings);
    }
}
