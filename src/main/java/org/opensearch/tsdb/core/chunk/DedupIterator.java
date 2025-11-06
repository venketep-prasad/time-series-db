/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.chunk;

import org.opensearch.tsdb.core.model.Sample;

import java.util.List;

/**
 * Wrapper iterator that handles duplicate timestamps from an underlying iterator.
 * Assumes the underlying iterator produces timestamps in non-decreasing order.
 */
public class DedupIterator implements ChunkIterator {

    /**
     * Policy for handling duplicate timestamps.
     */
    public enum DuplicatePolicy {
        /**
         * Keep the first value for each timestamp, skip subsequent duplicates
         */
        FIRST,
        /**
         * Keep the last value for each timestamp, overwrite previous values
         */
        LAST
    }

    /**
     * Internal strategy interface for deduplication logic.
     */
    private interface Strategy {
        ValueType next();
    }

    private final ChunkIterator underlying;
    private final Strategy strategy;
    private long currentTimestamp;
    private double currentValue;
    private Exception error;

    public DedupIterator(ChunkIterator underlying, DuplicatePolicy policy) {
        if (underlying == null) {
            throw new IllegalArgumentException("Underlying iterator cannot be null");
        }
        if (policy == null) {
            throw new IllegalArgumentException("DuplicatePolicy cannot be null");
        }
        this.underlying = underlying;

        this.strategy = switch (policy) {
            case FIRST:
                yield new KeepFirstStrategy();
            case LAST:
                yield new KeepLastStrategy();
        };
    }

    /**
     * Keep first value strategy - skip duplicates.
     */
    private final class KeepFirstStrategy implements Strategy {
        private long previousTimestamp = Long.MIN_VALUE;

        @Override
        public ValueType next() {
            while (true) {
                ValueType type = underlying.next();
                Exception err = underlying.error();
                if (err != null) {
                    error = err;
                }

                if (type == ValueType.NONE) {
                    return ValueType.NONE;
                }

                TimestampValue tv = underlying.at();
                long timestamp = tv.timestamp();

                if (timestamp != previousTimestamp) {
                    currentTimestamp = timestamp;
                    currentValue = tv.value();
                    previousTimestamp = timestamp;
                    return ValueType.FLOAT;
                }
            }
        }
    }

    /**
     * Keep last value strategy - read ahead to consume all duplicates.
     */
    private final class KeepLastStrategy implements Strategy {
        private TimestampValue pending = null;

        @Override
        public ValueType next() {
            TimestampValue toEmit = pending;
            pending = null;

            while (true) {
                ValueType type = underlying.next();
                Exception err = underlying.error();
                if (err != null) {
                    error = err;
                }

                if (type == ValueType.NONE) {
                    if (toEmit != null) {
                        currentTimestamp = toEmit.timestamp();
                        currentValue = toEmit.value();
                        return ValueType.FLOAT;
                    }
                    return ValueType.NONE;
                }

                TimestampValue tv = underlying.at();

                if (toEmit == null || tv.timestamp() == toEmit.timestamp()) {
                    toEmit = tv; // update value for duplicates
                } else {
                    currentTimestamp = toEmit.timestamp();
                    currentValue = toEmit.value();
                    pending = tv; // save next
                    return ValueType.FLOAT;
                }
            }
        }
    }

    @Override
    public ValueType next() {
        return strategy.next();
    }

    @Override
    public TimestampValue at() {
        return new TimestampValue(currentTimestamp, currentValue);
    }

    @Override
    public Exception error() {
        return error != null ? error : underlying.error();
    }

    @Override
    public int totalSamples() {
        // can't predict how many duplicates will be removed
        return -1;
    }

    @Override
    public List<Sample> decodeSamples(long minTimestamp, long maxTimestamp) {
        return ChunkIterator.super.decodeSamples(minTimestamp, maxTimestamp);
    }
}
