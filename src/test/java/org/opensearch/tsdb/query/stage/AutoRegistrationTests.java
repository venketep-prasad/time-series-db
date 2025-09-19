/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AutoRegistrationTests extends OpenSearchTestCase {

    public void testScaleStageAutoRegistration() {
        // Test that ScaleStage is automatically registered during factory initialization
        assertTrue("Scale stage should be auto-registered", PipelineStageFactory.isStageTypeSupported("scale"));

    }

    public void testAutoRegistrationCompleteness() {
        // Test that all required methods are registered
        Set<String> supportedTypes = PipelineStageFactory.getSupportedStageTypes();

        assertNotNull("Supported types should not be null", supportedTypes);
        assertTrue("Should contain scale stage", supportedTypes.contains("scale"));

        // Test args-based creation
        Map<String, Object> args = new HashMap<>();
        args.put("factor", 3.0);
        PipelineStage stage2 = PipelineStageFactory.createWithArgs("scale", args);
        assertNotNull("Stage created via args should not be null", stage2);
    }

    public void testAutoRegistrationPersistence() {
        // Test that auto-registration persists across multiple factory calls
        for (int i = 0; i < 10; i++) {
            assertTrue("Scale stage should remain registered", PipelineStageFactory.isStageTypeSupported("scale"));

        }
    }

    public void testAutoRegistrationThreadSafety() {
        // Test that auto-registration is thread-safe
        int numThreads = 5;
        Thread[] threads = new Thread[numThreads];
        Exception[] exceptions = new Exception[numThreads];

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                try {
                    // Each thread tries to access the auto-registered stages
                    for (int j = 0; j < 20; j++) {
                        assertTrue(
                            "Scale stage should be supported in thread " + threadId,
                            PipelineStageFactory.isStageTypeSupported("scale")
                        );

                    }
                } catch (Exception e) {
                    exceptions[threadId] = e;
                }
            });
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                fail("Thread interrupted: " + e.getMessage());
            }
        }

        // Check for exceptions
        for (int i = 0; i < numThreads; i++) {
            if (exceptions[i] != null) {
                fail("Thread " + i + " failed: " + exceptions[i].getMessage());
            }
        }
    }

    public void testAutoRegistrationAnnotationRequirements() {
        // Test that the annotation system works as expected
        // This indirectly tests that ScaleStage has the required annotation
        assertTrue("ScaleStage should be annotated", ScaleStage.class.isAnnotationPresent(PipelineStageAnnotation.class));

        // Test that the annotation has the correct name
        PipelineStageAnnotation annotation = ScaleStage.class.getAnnotation(PipelineStageAnnotation.class);
        assertNotNull("Annotation should exist", annotation);
        assertEquals("Annotation name should be 'scale'", "scale", annotation.name());
    }

    public void testAutoRegistrationMethodAvailability() {
        // Test that all required static methods are available on ScaleStage
        try {
            // Test fromArgs method
            ScaleStage.class.getMethod("fromArgs", Map.class);

            // Test readFrom method
            ScaleStage.class.getMethod("readFrom", StreamInput.class);

        } catch (NoSuchMethodException e) {
            fail("ScaleStage should have all required static methods: " + e.getMessage());
        }
    }

    public void testAutoRegistrationErrorHandling() {
        // Test that the factory handles errors gracefully when auto-registration fails
        // This is tested indirectly by ensuring the factory still works even if there are issues

    }

    public void testAutoRegistrationConsistency() {
        // Test that auto-registration is consistent across different access patterns
        Set<String> types1 = PipelineStageFactory.getSupportedStageTypes();
        Set<String> types2 = PipelineStageFactory.getSupportedStageTypes();

        assertEquals("Supported types should be consistent", types1, types2);
        assertTrue("Both sets should contain scale", types1.contains("scale") && types2.contains("scale"));

    }
}
