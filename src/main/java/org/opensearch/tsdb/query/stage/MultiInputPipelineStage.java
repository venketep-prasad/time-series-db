/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.List;
import java.util.Map;

/**
 * Interface for multi-input pipeline stages that operate on multiple time series inputs.
 *
 * <p>Multi-input pipeline stages perform operations that require combining or merging
 * data from multiple sources, where each source may have different characteristics
 * (e.g., different time ranges, different aggregation paths).
 *
 * <p>Unlike {@link BinaryPipelineStage} which has exactly two inputs (left and right),
 * multi-input stages can accept an arbitrary number of named inputs, each identified
 * by a reference name.</p>
 *
 * <h2>Common Multi-Input Operations:</h2>
 * <ul>
 *   <li><strong>Stitch:</strong> Combine time series from different time ranges into a continuous series</li>
 *   <li><strong>Merge:</strong> Merge time series from multiple sources with different characteristics</li>
 * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Stitch time series from different time ranges
 * MultiInputPipelineStage stitchStage = new StitchStage(
 *     Map.of(
 *         "R1", new StitchMetadata("R1", 1000L, 2000L),
 *         "R2", new StitchMetadata("R2", 2000L, 3000L)
 *     )
 * );
 * Map<String, List<TimeSeries>> inputs = Map.of(
 *     "R1", timeSeriesFromRange1,
 *     "R2", timeSeriesFromRange2
 * );
 * List<TimeSeries> stitched = stitchStage.process(inputs);
 * }</pre>
 *
 */
public interface MultiInputPipelineStage extends PipelineStage {

    /**
     * Process multiple time series inputs and return the result.
     *
     * <p>The input map contains named time series lists, where each entry represents
     * a different data source or aggregation result. The implementation should process
     * these inputs according to the stage's semantics.</p>
     *
     * @param inputs Map of reference names to their corresponding time series lists.
     *               All lists must be non-null (but may be empty).
     * @return The result time series after processing all inputs
     * @throws NullPointerException if inputs is null or contains null values
     * @throws IllegalArgumentException if required inputs are missing
     */
    List<TimeSeries> process(Map<String, List<TimeSeries>> inputs);

    /**
     * Get the names of all input references that this stage requires.
     *
     * <p>This method returns the set of reference names that must be present
     * in the inputs map when {@link #process(Map)} is called. The coordinator
     * uses this to validate that all required references are available and to
     * resolve them from the aggregation results.</p>
     *
     * @return List of required input reference names, never null
     */
    List<String> getInputReferences();

    /**
     * Multi-input pipeline stages require multiple inputs. Single-input processing is not supported.
     *
     * @throws UnsupportedOperationException always, since multi-input stages need multiple inputs
     */
    @Override
    default List<TimeSeries> process(List<TimeSeries> input) {
        throw new UnsupportedOperationException(
            "Multi-input pipeline stage '" + getName() + "' requires multiple inputs via process(Map<String, List<TimeSeries>>)"
        );
    }

    /**
     * Multi-input pipeline stages must be executed at the coordinator level.
     *
     * <p>Multi-input stages combine data from multiple aggregations that are only
     * available after shard-level processing completes. Therefore, they can only
     * execute at the coordinator level where all aggregation results are available.</p>
     *
     * @return true always, multi-input stages are coordinator-only
     */
    @Override
    default boolean isCoordinatorOnly() {
        return true;
    }
}
