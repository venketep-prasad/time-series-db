/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class SampleTypeTests extends OpenSearchTestCase {

    public void testSampleTypeSerialization() throws IOException {
        // Test FLOAT_SAMPLE
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            SampleType.FLOAT_SAMPLE.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                SampleType readType = SampleType.readFrom(in);
                assertEquals(SampleType.FLOAT_SAMPLE, readType);
                assertEquals((byte) 0, readType.getId());
            }
        }

        // Test SUM_COUNT_SAMPLE
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            SampleType.SUM_COUNT_SAMPLE.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                SampleType readType = SampleType.readFrom(in);
                assertEquals(SampleType.SUM_COUNT_SAMPLE, readType);
                assertEquals((byte) 1, readType.getId());
            }
        }
    }

    public void testFromId() {
        assertEquals(SampleType.FLOAT_SAMPLE, SampleType.fromId((byte) 0));
        assertEquals(SampleType.SUM_COUNT_SAMPLE, SampleType.fromId((byte) 1));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SampleType.fromId((byte) 99));
        assertEquals("Unknown sample type ID: 99", e.getMessage());
    }

    public void testSampleSerializationWithEnum() throws IOException {
        // Test FloatSample serialization
        FloatSample floatSample = new FloatSample(1000L, 42.5);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            floatSample.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                Sample readSample = Sample.readFrom(in);
                assertTrue(readSample instanceof FloatSample);
                assertEquals(1000L, readSample.getTimestamp());
                assertEquals(42.5, readSample.getValue(), 0.001);
                assertEquals(SampleType.FLOAT_SAMPLE, readSample.getSampleType());
            }
        }

        // Test SumCountSample serialization
        SumCountSample sumCountSample = new SumCountSample(2000L, 100.0, 5);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            sumCountSample.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                Sample readSample = Sample.readFrom(in);
                assertTrue(readSample instanceof SumCountSample);
                assertEquals(2000L, readSample.getTimestamp());
                assertEquals(20.0, readSample.getValue(), 0.001); // 100.0 / 5
                assertEquals(SampleType.SUM_COUNT_SAMPLE, readSample.getSampleType());
            }
        }
    }
}
