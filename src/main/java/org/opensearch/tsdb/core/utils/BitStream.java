/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.utils;

import java.util.Arrays;

/**
 * Utility class for writing individual bits to a byte stream.
 *
 * Provides bit-level writing operations commonly used in compression algorithms
 * for time series data. Supports writing variable-length bit sequences and
 * maintains position state for sequential writing operations.
 */
public class BitStream {
    private byte[] buffer;
    private int bytePos;
    private int bitPos; // 0–7

    private static final int DEFAULT_CAPACITY = 64; // Prometheus uses 128 bytes as default capacity, should we use that?

    /**
     * Constructs a new BitStream with default capacity.
     */
    public BitStream() {
        this.buffer = new byte[DEFAULT_CAPACITY];
        this.bytePos = 0;
        this.bitPos = 0;
    }

    /**
     * Writes a single bit to the stream.
     * @param bit the bit value to write (0 or 1)
     */
    public void writeBit(int bit) {
        writeBits(bit, 1);
    }

    /**
     * Writes the specified number of bits to the stream.
     * @param value the value to write
     * @param numBits the number of bits to write from the value
     */
    public void writeBits(long value, int numBits) {
        ensureCapacity((numBits + 7) / 8);

        // Shift value to align with most significant bits
        value <<= (64 - numBits);

        // Write whole bytes first
        while (numBits >= 8 && bitPos == 0) {
            byte byteValue = (byte) (value >>> 56);
            buffer[bytePos] = byteValue;
            bytePos++;
            value <<= 8;
            numBits -= 8;
        }

        // Write remaining bits individually
        while (numBits > 0) {
            if ((value >>> 63) != 0) {
                buffer[bytePos] |= (1 << (7 - bitPos));
            }
            bitPos++;
            if (bitPos == 8) {
                bitPos = 0;
                bytePos++;
            }
            value <<= 1;
            numBits--;
        }
    }

    /**
     * Returns the current size of the stream in bytes.
     * @return the size in bytes
     */
    public int size() {
        return bytePos + (bitPos > 0 ? 1 : 0);
    }

    /**
     * Writes a single byte to the stream.
     * @param value the byte value to write
     */
    public void writeByte(byte value) {
        writeBits(value & 0xFF, 8);
    }

    /**
     * Writes a variable-length signed integer to the stream.
     * @param value the signed integer value to write
     */
    public void writeVarint(long value) {
        // Convert signed to unsigned
        long uvalue = (value << 1) ^ (value >> 63);
        writeUvarint(uvalue);
    }

    /**
     * Writes a variable-length unsigned integer to the stream.
     * @param value the unsigned integer value to write
     */
    public void writeUvarint(long value) {
        while (value >= 0x80) {
            writeByte((byte) (value | 0x80));
            value >>>= 7;
        }
        writeByte((byte) value);
    }

    /**
     * Returns the stream contents as a byte array.
     * @return the byte array containing the stream data
     */
    public byte[] toByteArray() {
        int totalBytes = bytePos + (bitPos > 0 ? 1 : 0);
        return Arrays.copyOf(buffer, totalBytes);
    }

    private void ensureCapacity(int additionalBytes) {
        int required = bytePos + additionalBytes + 1;
        if (required > buffer.length) {
            buffer = Arrays.copyOf(buffer, Math.max(required, buffer.length * 2));
        }
    }

    /**
     * Updates a short value (2 bytes) at the specified offset in the buffer.
     * @param offset the offset to start updating from
     * @param value the short value to write
     * @throws IndexOutOfBoundsException if the update would exceed buffer bounds
     */
    public void updateShortAt(int offset, short value) {
        if (offset < 0 || offset + 1 >= buffer.length) {
            throw new IndexOutOfBoundsException("Short update would exceed buffer bounds");
        }
        buffer[offset] = (byte) (value >>> 8);     // High byte
        buffer[offset + 1] = (byte) (value & 0xFF); // Low byte
    }

    /**
     * Reads a short value (2 bytes) from the specified offset in the buffer.
     * @param offset the offset to read from
     * @return the short value read from the buffer
     * @throws IndexOutOfBoundsException if the read would exceed buffer bounds
     */
    public short readShortAt(int offset) {
        if (offset < 0 || offset + 1 >= buffer.length) {
            throw new IndexOutOfBoundsException("Short read would exceed buffer bounds");
        }
        return (short) ((buffer[offset] << 8) | (buffer[offset + 1] & 0xFF));
    }

}
