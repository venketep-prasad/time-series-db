/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark pipeline stage classes for automatic registration with the factory.
 *
 * <p>Classes annotated with this annotation will be automatically discovered and registered
 * with the PipelineStageFactory during initialization. This eliminates the need for manual
 * registration in the factory's static initializer block.</p>
 *
 * <h2>Usage:</h2>
 * <pre>{@code
 * @PipelineStageAnnotation(name = "scale")
 * public class ScaleStage implements UnaryPipelineStage {
 *     // Implementation...
 * }
 * }</pre>
 *
 * <h2>Requirements:</h2>
 * <ul>
 *   <li>The annotated class must implement {@link PipelineStage}</li>
 *   <li>The class must have a static {@code parse(String)} method</li>
 *   <li>The class must have a static {@code fromArgs(Map<String, Object>)} method</li>
 *   <li>The class must have a static {@code readFrom(StreamInput)} method</li>
 * </ul>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface PipelineStageAnnotation {

    /**
     * The name of the pipeline stage as it appears in definitions.
     *
     * @return the stage name
     */
    String name();
}
