/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.chunk;

import java.util.List;

/**
 * Iterator that merges multiple sorted ChunkIterators into a single sorted stream. Uses a min-heap to efficiently
 * merge k sorted iterators. Duplicates are preserved, and in case of equal timestamps, the order is preserved based on
 * the original iterator order.
 */
public class MergeIterator implements ChunkIterator {

    /**
     * Entry holding iterator state.
     */
    private static class IteratorEntry {
        ChunkIterator iterator;
        int index;
        long timestamp;
        double value;
        boolean hasValue;

        boolean advance() {
            ValueType type = iterator.next();
            if (type == ValueType.NONE) {
                hasValue = false;
                return false;
            }
            TimestampValue tv = iterator.at();
            timestamp = tv.timestamp();
            value = tv.value();
            hasValue = true;
            return true;
        }
    }

    private final IteratorEntry[] heap;
    private int heapSize;
    private long currentTimestamp;
    private double currentValue;
    private Exception error;
    private final int totalSamples;

    public MergeIterator(List<ChunkIterator> iterators) {
        if (iterators == null || iterators.isEmpty()) {
            throw new IllegalArgumentException("Iterators list cannot be null or empty");
        }

        heap = new IteratorEntry[iterators.size()];
        heapSize = 0;

        int sum = 0;
        boolean allKnown = true;

        int index = 0;
        for (ChunkIterator iter : iterators) {
            if (iter == null) {
                index++;
                continue;
            }

            Exception err = iter.error();
            if (err != null) {
                error = err;
                totalSamples = -1;
                return;
            }

            int samples = iter.totalSamples();
            if (samples >= 0) {
                sum += samples;
            } else {
                allKnown = false;
            }

            IteratorEntry entry = new IteratorEntry();
            entry.iterator = iter;
            entry.index = index;
            if (entry.advance()) {
                heap[heapSize++] = entry;
                siftUp(heapSize - 1);
            }

            if (iter.error() != null) {
                error = iter.error();
                totalSamples = -1;
                return;
            }

            index++;
        }

        totalSamples = allKnown ? sum : -1;
    }

    @Override
    public ValueType next() {
        if (error != null || heapSize == 0) {
            return ValueType.NONE;
        }

        // for k = 1, direct delegation to single iterator
        if (heapSize == 1) {
            IteratorEntry entry = heap[0];
            currentTimestamp = entry.timestamp;
            currentValue = entry.value;

            if (!entry.advance()) {
                heapSize = 0;
            }

            if (entry.iterator.error() != null) {
                error = entry.iterator.error();
            }

            return ValueType.FLOAT;
        }

        // for k = 2, use simple comparison instead of heap
        if (heapSize == 2) {
            IteratorEntry entry0 = heap[0];
            IteratorEntry entry1 = heap[1];

            IteratorEntry minEntry;
            // prefer lower timestamp, then lower index
            if (entry0.timestamp < entry1.timestamp || (entry0.timestamp == entry1.timestamp && entry0.index <= entry1.index)) {
                minEntry = entry0;
            } else {
                minEntry = entry1;
            }

            currentTimestamp = minEntry.timestamp;
            currentValue = minEntry.value;

            if (!minEntry.advance()) {
                // remove exhausted iterator
                if (minEntry == entry0) {
                    heap[0] = entry1;
                    heap[1] = null;
                } else {
                    heap[1] = null;
                }
                heapSize = 1;
            }

            if (minEntry.iterator.error() != null) {
                error = minEntry.iterator.error();
            }

            return ValueType.FLOAT;
        }

        // for k >= 3, min-heap approach
        IteratorEntry minEntry = heap[0];
        currentTimestamp = minEntry.timestamp;
        currentValue = minEntry.value;

        if (minEntry.advance()) {
            siftDown(0);
        } else {
            // remove exhausted iterator
            heapSize--;
            if (heapSize > 0) {
                heap[0] = heap[heapSize];
                heap[heapSize] = null;
                siftDown(0);
            }
        }

        if (minEntry.iterator.error() != null) {
            error = minEntry.iterator.error();
        }

        return ValueType.FLOAT;
    }

    @Override
    public TimestampValue at() {
        return new TimestampValue(currentTimestamp, currentValue);
    }

    @Override
    public Exception error() {
        return error;
    }

    @Override
    public int totalSamples() {
        return totalSamples;
    }

    private void siftUp(int index) {
        IteratorEntry entry = heap[index];
        long timestamp = entry.timestamp;
        int entryIndex = entry.index;

        while (index > 0) {
            int parent = (index - 1) >>> 1;
            IteratorEntry parentEntry = heap[parent];

            if (timestamp > parentEntry.timestamp) {
                break;
            }
            if (timestamp == parentEntry.timestamp && entryIndex >= parentEntry.index) {
                break;
            }

            heap[index] = parentEntry;
            index = parent;
        }

        heap[index] = entry;
    }

    private void siftDown(int index) {
        IteratorEntry entry = heap[index];
        long timestamp = entry.timestamp;
        int entryIndex = entry.index;
        int half = heapSize >>> 1;

        while (index < half) {
            int child = (index << 1) + 1;
            IteratorEntry childEntry = heap[child];
            int right = child + 1;

            if (right < heapSize) {
                IteratorEntry rightEntry = heap[right];
                if (rightEntry.timestamp < childEntry.timestamp
                    || (rightEntry.timestamp == childEntry.timestamp && rightEntry.index < childEntry.index)) {
                    child = right;
                    childEntry = rightEntry;
                }
            }

            if (timestamp < childEntry.timestamp) {
                break;
            }
            if (timestamp == childEntry.timestamp && entryIndex <= childEntry.index) {
                break;
            }

            heap[index] = childEntry;
            index = child;
        }

        heap[index] = entry;
    }
}
