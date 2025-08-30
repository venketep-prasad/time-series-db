/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.chunks;

import org.opensearch.tsdb.core.utils.BitReader;

import java.nio.ByteBuffer;

/**
 * Iterator for reading XOR-compressed chunk data
 */
public class XORIterator implements ChunkIterator {

    private BitReader bitReader;
    int totalSamples;
    private int samplesRead;

    // Current state
    long currentTimestamp;
    double currentValue;
    long timeDelta;

    private Exception error;

    // State flags
    private boolean isFirst = true;
    private boolean isSecond = false;

    // XOR-specific state
    byte leading;
    byte trailing;

    /**
     * Creates a new XORIterator for the given data.
     * @param data the XOR-compressed chunk data
     */
    public XORIterator(byte[] data) {
        reset(data);
    }

    /**
     * Resets this iterator to read from new data.
     * @param data the new XOR-compressed chunk data
     */
    public void reset(byte[] data) {
        this.samplesRead = 0;
        this.error = null;
        this.isFirst = true;
        this.isSecond = false;
        this.currentTimestamp = 0;
        this.currentValue = 0.0;
        this.timeDelta = 0;
        this.leading = (byte) 0xff;
        this.trailing = 0;

        try {
            if (data.length >= 2) {
                this.totalSamples = ByteBuffer.wrap(data, 0, 2).getShort() & 0xFFFF;
                // Skip the first 2 bytes (sample count) and create bit reader from remaining data
                this.bitReader = new BitReader(java.util.Arrays.copyOfRange(data, 2, data.length));
            } else {
                this.totalSamples = 0;
                this.bitReader = new BitReader(new byte[0]);
            }
        } catch (Exception e) {
            this.error = e;
            this.totalSamples = 0;
        }
    }

    @Override
    public final ChunkIterator.ValueType next() {
        if (error != null || samplesRead >= totalSamples) {
            return ChunkIterator.ValueType.NONE;
        }

        try {
            if (isFirst) {
                // First value: read timestamp and value directly
                currentTimestamp = bitReader.readVarint();
                currentValue = Double.longBitsToDouble(bitReader.readBits(64));

                isFirst = false;
                isSecond = true;
                samplesRead++;
                return ChunkIterator.ValueType.FLOAT;
            } else if (isSecond) {
                // Second value: read timestamp delta and value
                timeDelta = bitReader.readUvarint();
                currentTimestamp += timeDelta;
                readValue();
                isSecond = false;
            } else {
                // Subsequent values: read delta-of-delta for timestamp
                long deltaOfDelta = readTimestampDelta(bitReader);
                timeDelta = timeDelta + deltaOfDelta;
                currentTimestamp += timeDelta;
                readValue();
            }

            samplesRead++;
            return ChunkIterator.ValueType.FLOAT;

        } catch (Exception e) {
            this.error = e;
            return ChunkIterator.ValueType.NONE;
        }
    }

    @Override
    public final TimestampValue at() {
        return new TimestampValue(currentTimestamp, currentValue);
    }

    @Override
    public final Exception error() {
        return error;
    }

    /**
     * Read a value with decompression
     */
    protected void readValue() {
        readXOR();
    }

    private void readXOR() {
        int bit = bitReader.readBit();
        if (bit == 0) {
            // No change in value
            return;
        }

        bit = bitReader.readBit();

        byte newLeading, newTrailing;
        byte numBits;

        if (bit == 0) {
            // Reuse previous leading/trailing
            newLeading = leading;
            newTrailing = trailing;
            numBits = (byte) (64 - newLeading - newTrailing);
        } else {
            // Read new leading/trailing
            newLeading = (byte) bitReader.readBits(5);
            byte sigBits = (byte) bitReader.readBits(6);

            // Handle special case where 0 means 64 bits
            if (sigBits == 0) {
                sigBits = 64;
            }

            newTrailing = (byte) (64 - newLeading - sigBits);
            numBits = sigBits;

            // Update leading/trailing for next iteration
            leading = newLeading;
            trailing = newTrailing;
        }

        long valueBits = bitReader.readBits(numBits);
        long currentValueBits = Double.doubleToRawLongBits(currentValue);
        long newValueBits = currentValueBits ^ (valueBits << newTrailing);
        currentValue = Double.longBitsToDouble(newValueBits);
    }

    /**
     * Reads a timestamp delta-of-delta from the bit stream.
     * @param bitReader the bit reader to read from
     * @return the decoded delta-of-delta value
     */
    private long readTimestampDelta(BitReader bitReader) {
        byte d = 0;
        // read delta-of-delta header
        for (int i = 0; i < 4; i++) {
            d <<= 1;
            int bit = bitReader.readBit();
            if (bit == 0) {
                break;
            }
            d |= 1;
        }

        byte sz = 0;
        long dod = 0;
        switch (d) {
            case 0b0:
                // dod == 0
                break;
            case 0b10:
                sz = 14;
                break;
            case 0b110:
                sz = 17;
                break;
            case 0b1110:
                sz = 20;
                break;
            case 0b1111:
                long bits = bitReader.readBits(64);
                dod = bits;
                break;
            default:
                throw new IllegalStateException("Invalid delta-of-delta header: " + d);
        }

        if (sz != 0) {
            long bits = bitReader.readBits(sz);

            // Account for negative numbers, which come back as high unsigned numbers.
            if (bits > (1L << (sz - 1))) {
                bits -= 1L << sz;
            }
            dod = bits;
        }

        return dod;
    }

}
