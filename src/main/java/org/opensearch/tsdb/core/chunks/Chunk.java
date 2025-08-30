/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.chunks;

/**
 * A chunk represents a compressed collection of time series samples.
 * <p>
 * Chunks are the fundamental unit of time series data storage, containing
 * multiple timestamp-value pairs in a compressed format optimized for
 * efficient storage and retrieval of time series data.
 */
public interface Chunk {
    /**
     * Returns the underlying bytes of the chunk.
     * @return the underlying bytes of the chunk. May perform copy, use bytesSize() if you only need the size.
     */
    byte[] bytes();

    /**
     * Returns the size of the chunk in bytes.
     * @return the size in bytes
     */
    int bytesSize();

    /**
     * Returns the encoding type used by this chunk.
     * @return the encoding type
     */
    Encoding encoding();

    /**
     * Returns an appender for adding new samples to this chunk.
     * @return the chunk appender
     */
    ChunkAppender appender();

    /**
     * Returns the number of samples stored in this chunk.
     * @return the number of samples
     */
    int numSamples();

    /**
     * Returns an iterator for reading data from this chunk
     * @return iterator for reading chunk data
     */
    ChunkIterator iterator();

}
