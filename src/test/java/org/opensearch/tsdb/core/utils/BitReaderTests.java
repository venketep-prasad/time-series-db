/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.utils;

import org.opensearch.test.OpenSearchTestCase;

public class BitReaderTests extends OpenSearchTestCase {

    public void testReadBit() {
        byte[] data = { (byte) 0b10101010, (byte) 0b11110000 };
        BitReader reader = new BitReader(data);

        assertEquals("First bit should be 1", 1, reader.readBit());
        assertEquals("Second bit should be 0", 0, reader.readBit());
        assertEquals("Third bit should be 1", 1, reader.readBit());
        assertEquals("Fourth bit should be 0", 0, reader.readBit());
    }

    public void testReadBits() {
        byte[] data = { (byte) 0xFF, (byte) 0x00, (byte) 0xAA };
        BitReader reader = new BitReader(data);

        assertEquals("Should read 8 bits correctly", 0xFF, reader.readBits(8));
        assertEquals("Should read 4 bits correctly", 0x0, reader.readBits(4));
        assertEquals("Should read 12 bits correctly", 0x0AA, reader.readBits(12));
    }

    public void testReadVarint() {
        BitStream stream = new BitStream();
        stream.writeVarint(42);
        stream.writeVarint(-42);
        stream.writeVarint(0);

        BitReader reader = new BitReader(stream.toByteArray());
        assertEquals("Should read positive varint", 42, reader.readVarint());
        assertEquals("Should read negative varint", -42, reader.readVarint());
        assertEquals("Should read zero varint", 0, reader.readVarint());
    }

    public void testReadUvarint() {
        BitStream stream = new BitStream();
        stream.writeUvarint(127);
        stream.writeUvarint(128);
        stream.writeUvarint(16383);

        BitReader reader = new BitReader(stream.toByteArray());
        assertEquals("Should read small uvarint", 127, reader.readUvarint());
        assertEquals("Should read medium uvarint", 128, reader.readUvarint());
        assertEquals("Should read large uvarint", 16383, reader.readUvarint());
    }

    public void testEndOfStreamHandling() {
        byte[] data = { (byte) 0xFF };
        BitReader reader = new BitReader(data);

        // Read all 8 bits
        reader.readBits(8);

        // Try to read beyond end of stream
        try {
            reader.readBit();
            fail("Should throw IllegalStateException at end of stream");
        } catch (IllegalStateException e) {
            assertEquals("End of stream reached", e.getMessage());
        }
    }
}
