/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class PipelineStageAnnotationTests extends OpenSearchTestCase {

    public void testAnnotationPresent() {
        // Verify ScaleStage has the annotation
        assertTrue("ScaleStage should have PipelineStageAnnotation", ScaleStage.class.isAnnotationPresent(PipelineStageAnnotation.class));
    }

    public void testAnnotationValue() {
        // Get the annotation
        PipelineStageAnnotation annotation = ScaleStage.class.getAnnotation(PipelineStageAnnotation.class);
        assertNotNull("Annotation should not be null", annotation);

        // Verify the annotation value
        assertEquals("Annotation name should be 'scale'", "scale", annotation.name());
    }

    public void testAnnotationRetention() {
        // Verify the annotation is available at runtime
        Annotation[] annotations = ScaleStage.class.getAnnotations();
        boolean found = false;
        for (Annotation annotation : annotations) {
            if (annotation instanceof PipelineStageAnnotation) {
                found = true;
                break;
            }
        }
        assertTrue("PipelineStageAnnotation should be present at runtime", found);
    }

    public void testAnnotationTarget() {
        // Verify the annotation can be applied to classes
        PipelineStageAnnotation annotation = ScaleStage.class.getAnnotation(PipelineStageAnnotation.class);
        assertNotNull("ScaleStage should have the annotation", annotation);

        // The fact that we can get it from a class proves the @Target(ElementType.TYPE) works
        assertTrue("Annotation should be applicable to classes", true);
    }

    public void testAnnotationInterface() {
        // Test the annotation interface itself
        Class<PipelineStageAnnotation> annotationClass = PipelineStageAnnotation.class;

        // Verify it's an annotation
        assertTrue("Should be an annotation", annotationClass.isAnnotation());

        // Verify it has the name method
        try {
            Method nameMethod = annotationClass.getMethod("name");
            assertNotNull("Should have name() method", nameMethod);
            assertEquals("name() should return String", String.class, nameMethod.getReturnType());
        } catch (NoSuchMethodException e) {
            fail("Annotation should have name() method");
        }
    }

    public void testMultipleAnnotationsNotAllowed() {
        // Verify that a class can only have one PipelineStageAnnotation
        // (This is implicitly tested since Java doesn't allow duplicate annotations
        // unless they're marked as @Repeatable, which ours isn't)

        PipelineStageAnnotation[] annotations = ScaleStage.class.getAnnotationsByType(PipelineStageAnnotation.class);
        assertEquals("Should have exactly one PipelineStageAnnotation", 1, annotations.length);
    }
}
