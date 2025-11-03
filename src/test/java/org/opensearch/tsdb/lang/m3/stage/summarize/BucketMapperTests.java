/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.summarize;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SummarizePlanNode;

/**
 * Unit tests for BucketMapper.
 */
public class BucketMapperTests extends OpenSearchTestCase {

    /**
     * Test that Go's zero time reference works correctly.
     */
    public void testGoZeroTimeReference() {
        long interval = 3600000L; // 1 hour in milliseconds
        BucketMapper mapper = new BucketMapper(interval, SummarizePlanNode.GO_ZERO_TIME_MILLIS);

        // Go's zero time is January 1, year 1, 00:00:00.000000000 UTC
        // which corresponds to -62135596800000L milliseconds from Unix epoch
        assertEquals(SummarizePlanNode.GO_ZERO_TIME_MILLIS, mapper.getReferenceTime());
    }

    /**
     * Test that custom reference time is preserved.
     */
    public void testCustomReferenceTime() {
        long interval = 1000L;
        long customRef = 1609459200000L; // 2021-01-01 00:00:00 UTC
        BucketMapper mapper = new BucketMapper(interval, customRef);

        assertEquals(customRef, mapper.getReferenceTime());
        assertEquals(interval, mapper.getInterval());
    }

    /**
     * Test bucket mapping with Go's zero time reference.
     */
    public void testBucketMappingWithGoZeroTime() {
        long interval = 3600000L; // 1 hour
        BucketMapper mapper = new BucketMapper(interval, SummarizePlanNode.GO_ZERO_TIME_MILLIS);

        // Test Unix epoch (1970-01-01 00:00:00 UTC) maps correctly
        long unixEpoch = 0L;
        long bucketStart = mapper.mapToBucket(unixEpoch);

        // Since Go's zero time is much earlier than Unix epoch,
        // Unix epoch should map to a bucket that starts at an hour boundary
        assertEquals(0L, bucketStart % interval);
    }

    /**
     * Test invalid interval handling.
     */
    public void testInvalidInterval() {
        expectThrows(IllegalArgumentException.class, () -> new BucketMapper(0, 0L));
        expectThrows(IllegalArgumentException.class, () -> new BucketMapper(-1000, 0L));
    }

    /**
     * Test bucket boundaries calculation.
     */
    public void testBucketBoundariesCalculation() {
        long interval = 300000L; // 5 minutes
        long referenceTime = 1609459200000L; // 2021-01-01 00:00:00 UTC
        BucketMapper mapper = new BucketMapper(interval, referenceTime);

        long timestamp = 1609459200000L; // 2021-01-01 00:00:00 UTC

        long bucketStart = mapper.calculateBucketStart(timestamp);
        long bucketEnd = mapper.calculateBucketEnd(timestamp);

        // Bucket end should be exactly one interval after bucket start
        assertEquals(interval, bucketEnd - bucketStart);

        // The timestamp should fall within the bucket
        assertTrue(timestamp >= bucketStart);
        assertTrue(timestamp < bucketEnd);
    }
}
