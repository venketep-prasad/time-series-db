/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.tsdb.core.reader.TSDBDocValues;

/**
 * ClosedChunkIndexTSDBDocValues is a wrapper class for holding chunk doc values and labels doc values for closed chunk index.
 */
public class ClosedChunkIndexTSDBDocValues extends TSDBDocValues {
    /**
     * Constructor for closed chunk index tsdb doc values.
     *
     * @param chunkDocValues the binary doc values containing serialized chunk data
     * @param labelsDocValues the sorted set doc values containing labels
     */
    public ClosedChunkIndexTSDBDocValues(BinaryDocValues chunkDocValues, SortedSetDocValues labelsDocValues) {
        super(chunkDocValues, labelsDocValues);
    }

    @Override
    public NumericDocValues getChunkRefDocValues() {
        throw new UnsupportedOperationException("Closed Chunk Index does not support chunk references");
    }

    @Override
    public BinaryDocValues getChunkDocValues() {
        return this.chunkDocValues;
    }
}
