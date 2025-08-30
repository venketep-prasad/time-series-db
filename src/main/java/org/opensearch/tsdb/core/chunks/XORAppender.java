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
 * A XOR appender that implements delta-of-delta encoding in Facebook's Gorilla paper:
 * <a href="https://www.vldb.org/pvldb/vol8/p1816-teller.pdf">Gorilla: A Fast, Scalable, In-Memory Time Series Database</a>.
 */
public class XORAppender implements ChunkAppender {
    private byte leading;
    private byte trailing;

    private final XORChunk chunk;
    private long lastTimestamp;
    private double lastValue;
    private long timeDelta;

    // State for managing first and second values
    private boolean isFirst = true;
    private boolean isSecond = false;

    /**
     * Creates a XORAppender for an existing chunk with preserved state.
     * @param chunk the XOR chunk to append to
     * @param lastTimestamp the last timestamp written to the chunk
     * @param lastValue the last value written to the chunk
     * @param timeDelta the current time delta between samples
     * @param leading the current leading zero count for XOR compression
     * @param trailing the current trailing zero count for XOR compression
     */
    public XORAppender(XORChunk chunk, long lastTimestamp, double lastValue, long timeDelta, byte leading, byte trailing) {
        this.chunk = chunk;
        this.lastTimestamp = lastTimestamp;
        this.lastValue = lastValue;
        this.timeDelta = timeDelta;
        this.leading = leading;
        this.trailing = trailing;
        int numSamples = chunk.numSamples();
        this.isFirst = (numSamples == 0);
        this.isSecond = (numSamples == 1);
    }

    @Override
    public final void append(long timestamp, double value) {
        if (chunk == null) {
            throw new IllegalStateException("Appender not properly initialized with chunk");
        }

        BitStream bitStream = chunk.getBitStream();

        if (isFirst) {
            // First value: store timestamp and value directly
            bitStream.writeVarint(timestamp);
            bitStream.writeBits(Double.doubleToRawLongBits(value), 64);

            // Update sample count
            updateSampleCount(1);

            lastTimestamp = timestamp;
            lastValue = value;
            isFirst = false;
            isSecond = true;
            return;
        } else if (isSecond) {
            // Second value: write timestamp delta and value
            timeDelta = timestamp - lastTimestamp;
            bitStream.writeUvarint(timeDelta);
            writeValue(value);
            isSecond = false;
        } else {
            // Subsequent values: use delta-of-delta for timestamp
            long tDelta = timestamp - lastTimestamp;
            long deltaOfDelta = tDelta - timeDelta;
            writeTimestampDelta(bitStream, deltaOfDelta);
            writeValue(value);
            timeDelta = tDelta;
        }

        lastTimestamp = timestamp;
        lastValue = value;

        // Update sample count
        updateSampleCount(getSampleCount() + 1);
    }

    /**
     * Get current sample count from chunk header.
     * @return the current number of samples in the chunk
     */
    protected final int getSampleCount() {
        return chunk.numSamples();
    }

    /**
     * Update sample count in chunk header.
     * @param count the new sample count
     */
    protected final void updateSampleCount(int count) {
        chunk.getBitStream().updateShortAt(0, (short) count);
    }

    /**
     * Write a value with compression.
     * @param value the value to write
     */
    protected void writeValue(double value) {
        writeXOR(value, lastValue);
    }

    private void writeXOR(double newValue, double currentValue) {
        long newBits = Double.doubleToRawLongBits(newValue);
        long currentBits = Double.doubleToRawLongBits(currentValue);
        long delta = newBits ^ currentBits;

        if (delta == 0) {
            chunk.getBitStream().writeBit(0);
            return;
        }

        chunk.getBitStream().writeBit(1);

        byte newLeading = (byte) Long.numberOfLeadingZeros(delta);
        byte newTrailing = (byte) Long.numberOfTrailingZeros(delta);

        // Clamp leading zeros to avoid overflow when encoding
        if (newLeading >= 32) {
            newLeading = 31;
        }

        if (leading != (byte) 0xFF && newLeading >= leading && newTrailing >= trailing) {
            // Reuse previous leading/trailing
            chunk.getBitStream().writeBit(0);
            int numBits = 64 - leading - trailing;
            chunk.getBitStream().writeBits(delta >>> trailing, numBits);
        } else {
            // Update leading/trailing
            leading = newLeading;
            trailing = newTrailing;

            chunk.getBitStream().writeBit(1);
            chunk.getBitStream().writeBits(newLeading, 5);

            int sigBits = 64 - newLeading - newTrailing;
            // Handle special case where sigBits would be 64 (doesn't fit in 6 bits)
            if (sigBits == 64) {
                chunk.getBitStream().writeBits(0, 6);
            } else {
                chunk.getBitStream().writeBits(sigBits, 6);
            }
            chunk.getBitStream().writeBits(delta >>> newTrailing, sigBits);
        }
    }

    /**
     * Writes a timestamp delta-of-delta using variable-length encoding.
     * @param bitStream the bit stream to write to
     * @param deltaOfDelta the delta-of-delta value to encode
     */
    private void writeTimestampDelta(BitStream bitStream, long deltaOfDelta) {
        if (deltaOfDelta == 0) {
            bitStream.writeBit(0);                    // 1 bit: no change
        } else if (bitRange(deltaOfDelta, 14)) {
            bitStream.writeBits(0b10L << 6 | (deltaOfDelta >> 8 & 0x3FL), 8);  // 2+6 bits
            bitStream.writeBits(deltaOfDelta & 0xFFL, 8);                      // +8 bits = 16 total
        } else if (bitRange(deltaOfDelta, 17)) {
            bitStream.writeBits(0b110, 3);            // 3 bits: header
            bitStream.writeBits(deltaOfDelta, 17);    // 17 bits: delta = 20 total
        } else if (bitRange(deltaOfDelta, 20)) {
            bitStream.writeBits(0b1110, 4);           // 4 bits: header
            bitStream.writeBits(deltaOfDelta, 20);    // 20 bits: delta = 24 total
        } else {
            bitStream.writeBits(0b1111, 4);           // 4 bits: header
            bitStream.writeBits(deltaOfDelta, 64);    // 64 bits: full delta = 68 total
        }
    }

    /**
     * Check if a value can be represented in the given number of bits (signed).
     * @param value the value to check
     * @param numBits number of bits available
     * @return true if the value fits in the specified number of bits
     */
    private static boolean bitRange(long value, int numBits) {
        long max = (1L << (numBits - 1)) - 1;
        long min = -(1L << (numBits - 1)) + 1;
        return value >= min && value <= max;
    }

}
