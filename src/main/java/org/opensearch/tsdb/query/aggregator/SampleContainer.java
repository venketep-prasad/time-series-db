/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.tsdb.core.model.Sample;

import java.util.Iterator;

/**
 * A container interface for storing and accessing time series samples.
 *
 * <p>This interface provides a flexible abstraction for sample storage that supports
 * time-based access patterns. It is designed to accommodate different
 * storage strategies (dense arrays, sparse maps, etc.) while providing a consistent API
 * for time series data manipulation.
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li><b>Standard Access Modes:</b> Supports timestamp-based lookups</li>
 *   <li><b>Iterable:</b> Implements Iterable for easy traversal of samples</li>
 *   <li><b>Dynamic Growth:</b> Supports appending new samples</li>
 *   <li><b>Efficient Retrieval:</b> Optimized for both random time based access and sequential iteration</li>
 * </ul>
 *
 * <h2>Design Considerations:</h2>
 * <p>Implementations should consider the following trade-offs:
 * <ul>
 *   <li><b>Dense vs Sparse:</b> Dense implementations (arrays) are efficient for regular time series,
 *       while sparse implementations (maps) are better for irregular or sparse data</li>
 *   <li><b>Memory vs Speed:</b> Index-based access may require additional bookkeeping but
 *       provides O(1) access time</li>
 *   <li><b>Mutability:</b> Implementations may be immutable or mutable depending on use case</li>
 * </ul>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * // Create and populate a sample container
 * SampleContainer container = new DenseSampleContainer(startTime, endTime, step);
 * container.append(new FloatSample(1000L, 10.0));
 * container.append(new FloatSample(2000L, 20.0));
 * container.append(new FloatSample(3000L, 30.0));
 *
 * // Time-based access
 * double value = container.getValueFor(2000L);  // Returns 20.0
 *
 * // Iteration
 * for (Sample sample : container) {
 *     System.out.println(sample.getTimestamp() + ": " + sample.getValue());
 * }
 *
 * // Size check
 * long sampleCount = container.size();  // Returns 3
 * }</pre>
 *
 * <h2>Thread Safety:</h2>
 * <p>Implementations are not required to be thread-safe. Callers must provide external
 * synchronization if the container is accessed from multiple threads concurrently.
 *
 * @see Sample
 * @see org.opensearch.tsdb.core.model.FloatSample
 */
public interface SampleContainer extends Iterable<Sample> {

    /**
     * Retrieves the sample value for the specified timestamp.
     *
     * <p>This method provides time-based lookup of sample values. The exact behavior
     * for missing timestamps depends on the implementation:
     * <ul>
     *   <li>Some implementations may return NaN for missing timestamps</li>
     *   <li>Dense implementations may interpolate or return the nearest value</li>
     *   <li>Sparse implementations may throw an exception or return a sentinel value</li>
     * </ul>
     *
     * <p><b>Performance:</b> Time complexity varies by implementation:
     * <ul>
     *   <li>Dense containers: O(1)</li>
     *   <li>Sparse containers: O(1) for hash-based, O(log n) for tree-based</li>
     * </ul>
     *
     * @param ts the timestamp (in milliseconds) to query
     * @return the sample value at the specified timestamp
     * @throws IllegalArgumentException if the timestamp is outside the valid range for this container or no sample or not sample exist at the ts
     */
    Sample getSampleFor(long ts);

    /**
     * Updates the sample at the specified timestamp with the provided {@code Sample} object.
     *
     * <p>
     * If a sample with the given timestamp already exists in the container, it will be replaced
     * by the provided sample. If no sample exists for the specified timestamp, the exact behavior
     * depends on the implementation: some implementations may insert the new sample, while others
     * may throw an exception or ignore the update.
     * </p>
     *
     * <p>
     * Performance characteristics and support for updating non-existent samples are
     * implementation-specific.
     * </p>
     *
     * @param ts     the timestamp (in milliseconds) of the sample to update
     * @param sample the new {@code Sample} object to associate with the given timestamp
     * @throws IllegalArgumentException if the timestamp is outside the acceptable range,
     *                                  or if updates are not allowed
     *                                  by the implementation
     */

    void updateSampleFor(long ts, Sample sample);

    /**
     * Returns an iterator over the samples in this container.
     *
     * <p>The iterator traverses samples in chronological order (by timestamp).
     * Implementations should provide an efficient iterator that doesn't require
     * materializing all samples into a temporary collection.
     *
     * <p><b>Iterator Behavior:</b>
     * <ul>
     *   <li>Samples are returned in ascending timestamp order</li>
     *   <li>The iterator reflects the container state at the time of creation</li>
     *   <li>Modifications to the container during iteration may result in
     *       {@link java.util.ConcurrentModificationException} (implementation-dependent)</li>
     * </ul>
     *
     * <p><b>Performance:</b> Iterator creation should be O(1). The {@code next()} operation should be
     * O(1) amortized for most implementations.
     *
     * <p><b>Usage Example:</b>
     * <pre>{@code
     * // Explicit iterator usage
     * Iterator<Sample> iter = container.iterator();
     * while (iter.hasNext()) {
     *     Sample sample = iter.next();
     *     processample(sample);
     * }
     *
     * // Enhanced for-loop (recommended)
     * for (Sample sample : container) {
     *     processSample(sample);
     * }
     * }</pre>
     *
     * @return an iterator over the samples in chronological order
     */
    @Override
    Iterator<Sample> iterator();

    /**
     * Appends a new sample to this container.
     *
     * <p>This method adds a sample to the container. Sample is required to have a timestamp greater
     * than or equal to the last appended sample (for ordered containers).
     *
     * <p><b>Ordering Requirements:</b> Most implementations expect samples to be appended in chronological order:
     * <ul>
     *   <li>Ordered implementations may throw an exception if a sample with an earlier
     *       timestamp is appended after a later one or sample is not appended at the expected timestamp</li>
     * </ul>
     *
     *
     * <p><b>Performance:</b> Time complexity varies by implementation:
     * <ul>
     *   <li>Array-based: O(1) amortized if appending to end, O(n) if requires shifting</li>
     *   <li>List-based: O(1) for end insertion</li>
     *   <li>Map-based: O(1) for hash-based, O(log n) for tree-based</li>
     * </ul>
     *
     * @param ts     the timestamp (in milliseconds) of the sample to update
     * @param sample the sample to append (must not be null)
     * @throws NullPointerException     if {@code sample} is null
     * @throws IllegalArgumentException if the sample's timestamp violates ordering constraints
     *                                  (implementation-dependent)
     * @throws IllegalStateException    if the container is immutable or full
     *                                  (implementation-dependent)
     */
    void append(long ts, Sample sample);

    /**
     * Returns the number of samples in this container.
     *
     * <p>This method returns the total count of samples currently stored in the container.
     *
     * <p><b>Performance:</b> Most implementations should provide O(1) time complexity by maintaining
     * a size counter. However, some lazy implementations might require O(n) traversal.
     *
     * <p><b>Return Type:</b> The return type is {@code long} to support containers with more than
     * {@link Integer#MAX_VALUE} samples, though in practice most time series will
     * have significantly fewer samples.
     *
     * @return the number of samples in this container (always >= 0)
     */
    long size();

    /**
     * Returns the min timestamp of all the samples in the container
     *
     * @return timestamp in milliseconds.
     */
    long getMinTimestamp();

    /**
     * Returns the max timestamp of all the samples in the container
     *
     * @return timestamp in milliseconds.
     */
    long getMaxTimestamp();
}
