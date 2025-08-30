/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.utils;

import org.opensearch.test.OpenSearchTestCase;

public class BitStreamTests extends OpenSearchTestCase {

    public void testWriteAndReadBits() {
        BitStream stream = new BitStream();

        stream.writeBit(1);
        stream.writeBit(0);
        stream.writeBits(0xAB, 8);

        byte[] data = stream.toByteArray();
        assertEquals("Should have 2 bytes", 2, data.length);

        BitReader reader = new BitReader(data);
        assertEquals("First bit should be 1", 1, reader.readBit());
        assertEquals("Second bit should be 0", 0, reader.readBit());
        assertEquals("Next 8 bits should be 0xAB", 0xAB, reader.readBits(8));
    }

    public void testVarintRoundTrip() {
        BitStream stream = new BitStream();
        long[] values = { 0, 1, -1, 42, -42, 127, -128, 16383, -16384 };

        for (long value : values) {
            stream.writeVarint(value);
        }

        BitReader reader = new BitReader(stream.toByteArray());
        for (long expected : values) {
            assertEquals("Varint round trip failed for " + expected, expected, reader.readVarint());
        }
    }

    public void testUvarintRoundTrip() {
        BitStream stream = new BitStream();
        long[] values = { 0, 1, 127, 128, 16383, 16384 };

        for (long value : values) {
            stream.writeUvarint(value);
        }

        BitReader reader = new BitReader(stream.toByteArray());
        for (long expected : values) {
            assertEquals("Uvarint round trip failed for " + expected, expected, reader.readUvarint());
        }
    }

    public void testSizeCalculation() {
        BitStream stream = new BitStream();
        assertEquals("Empty stream should have size 0", 0, stream.size());

        stream.writeBit(1);
        assertEquals("1 bit should result in size 1", 1, stream.size());

        stream.writeBits(0xFF, 7); // Total 8 bits = 1 byte
        assertEquals("8 bits should result in size 1", 1, stream.size());

        stream.writeBit(1); // 9th bit
        assertEquals("9 bits should result in size 2", 2, stream.size());
    }

    public void testShortOperations() {
        BitStream stream = new BitStream();
        stream.writeBits(0, 16); // Make space for shorts

        stream.updateShortAt(0, (short) 0x1234);
        assertEquals("Should read back correct short", (short) 0x1234, stream.readShortAt(0));

        stream.updateShortAt(0, (short) -1);
        assertEquals("Should handle negative short", (short) -1, stream.readShortAt(0));
    }

    public void testShortOperationsBounds() {
        BitStream stream = new BitStream();

        // Test bounds checking for negative offset
        try {
            stream.updateShortAt(-1, (short) 42);
            fail("Should throw IndexOutOfBoundsException for negative offset");
        } catch (IndexOutOfBoundsException e) {
            assertTrue("Should mention buffer bounds", e.getMessage().contains("buffer bounds"));
        }

        // Write some data and test valid operations
        stream.writeBits(0, 16); // Write 2 bytes
        stream.updateShortAt(0, (short) 0x1234);
        assertEquals("Should read correct value", (short) 0x1234, stream.readShortAt(0));

        // Test reading beyond buffer bounds
        try {
            stream.readShortAt(63); // Default capacity is 64, so offset 63 would try to read bytes 63,64
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            assertTrue("Should mention buffer bounds", e.getMessage().contains("buffer bounds"));
        }
    }

    public void testCapacityExpansion() {
        BitStream stream = new BitStream();

        // Write enough data to trigger expansion
        for (int i = 0; i < 100; i++) {
            stream.writeByte((byte) i);
        }

        assertTrue("Stream should expand beyond default capacity", stream.size() > 64);
        assertEquals("Size should match bytes written", 100, stream.size());

        byte[] data = stream.toByteArray();
        assertEquals("Array length should match size", stream.size(), data.length);
    }
}
