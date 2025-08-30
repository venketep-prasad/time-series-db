/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.chunks;

import org.opensearch.tsdb.core.utils.BitStream;

/**
 * Compressed chunk implementation using XOR compression algorithm.
 *
 * XOR chunks use the XOR compression algorithm optimized for time series data,
 * which exploits the typically small differences between consecutive timestamps
 * and values to achieve high compression ratios.
 */
public class XORChunk implements Chunk {

    private final BitStream bitStream;

    /**
     * Constructor for new chunk
     */
    public XORChunk() {
        this.bitStream = new BitStream();
        // Initialize with 2-byte header for sample count
        bitStream.writeBits(0, 16); // Initial sample count = 0
    }

    @Override
    public byte[] bytes() {
        return bitStream.toByteArray();
    }

    @Override
    public int bytesSize() {
        return bitStream.size();
    }

    @Override
    public int numSamples() {
        return bitStream.readShortAt(0) & 0xFFFF; // Convert to unsigned
    }

    /**
     * Provides access to the underlying BitStream.
     * @return the BitStream containing the compressed data
     */
    public BitStream getBitStream() {
        return bitStream;
    }

    @Override
    public Encoding encoding() {
        return Encoding.XOR;
    }

    @Override
    public ChunkAppender appender() {
        XORIterator it = new XORIterator(bytes());

        // To get an appender we must know the state it would have if we had
        // appended all existing data from scratch.
        // We iterate through the end and populate via the iterator's state.
        while (it.next() != ChunkIterator.ValueType.NONE) {
        }
        if (it.error() != null) {
            throw new RuntimeException("Error reading existing chunk data", it.error());
        }

        // TODO: consider how to handle concurrent appends
        return new XORAppender(this, it.currentTimestamp, it.currentValue, it.timeDelta, it.leading, it.trailing);
    }

    @Override
    public ChunkIterator iterator() {
        return new XORIterator(bytes());
    }
}
