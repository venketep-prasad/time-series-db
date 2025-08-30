/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.utils;

/**
 * Utility class for reading individual bits from a byte array.
 *
 * Provides bit-level reading operations commonly used in compression algorithms
 * for time series data.
 */
public class BitReader {
    private final byte[] buffer;
    private int bytePos;
    private int bitPos; // 0–7

    /**
     * Constructs a new BitReader for reading from the given byte array.
     * @param bytes the byte array to read from
     */
    public BitReader(byte[] bytes) {
        this.buffer = bytes;
        this.bytePos = 0;
        this.bitPos = 0;
    }

    /**
     * Reads a single bit from the stream.
     * @return the bit value (0 or 1)
     * @throws IllegalStateException if end of stream is reached
     */
    public int readBit() {
        if (bytePos >= buffer.length) {
            throw new IllegalStateException("End of stream reached");
        }

        int bit = (buffer[bytePos] >> (7 - bitPos)) & 1;
        bitPos++;
        if (bitPos == 8) {
            bitPos = 0;
            bytePos++;
        }
        return bit;
    }

    /**
     * Reads the specified number of bits from the stream.
     * @param numBits the number of bits to read (maximum 64)
     * @return the bits as a long value
     * @throws IllegalArgumentException if numBits > 64
     */
    public long readBits(int numBits) {
        if (numBits > 64) {
            throw new IllegalArgumentException("Cannot read more than 64 bits");
        }

        long value = 0;
        for (int i = 0; i < numBits; i++) {
            value = (value << 1) | readBit();
        }
        return value;
    }

    /**
     * Reads a variable-length signed integer from the stream.
     * @return the signed integer value
     * @throws IllegalStateException if end of stream is reached or overflow occurs
     */
    public long readVarint() {
        long value = readUvarint();
        // Convert unsigned to signed
        return (value >>> 1) ^ (-(value & 1));
    }

    /**
     * Reads a variable-length unsigned integer from the stream.
     * @return the unsigned integer value
     * @throws IllegalStateException if end of stream is reached or overflow occurs
     */
    public long readUvarint() {
        long value = 0;
        int shift = 0;

        while (true) {
            if (bytePos >= buffer.length) {
                throw new IllegalStateException("End of stream reached while reading uvarint");
            }

            byte b = (byte) readBits(8);
            if ((b & 0x80) == 0) {
                // Most significant bit is 0, this is the last byte
                if (shift == 63 && b > 1) {
                    throw new IllegalStateException("Uvarint overflow");
                }
                value |= ((long) b) << shift;
                break;
            }

            value |= ((long) (b & 0x7F)) << shift;
            shift += 7;

            if (shift >= 64) {
                throw new IllegalStateException("Uvarint overflow");
            }
        }

        return value;
    }
}
