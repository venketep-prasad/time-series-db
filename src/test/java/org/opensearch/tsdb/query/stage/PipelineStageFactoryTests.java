/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

public class PipelineStageFactoryTests extends OpenSearchTestCase {

    public void testCreateWithArgsValidName() {
        // Arrange
        String stageName = "scale";
        Map<String, Object> args = Map.of("factor", 2.0);

        // Act
        PipelineStage stage = PipelineStageFactory.createWithArgs(stageName, args);

        // Assert
        assertNotNull(stage);
        assertTrue(stage instanceof ScaleStage);
        assertEquals("scale", stage.getName());
        assertEquals(2.0, ((ScaleStage) stage).getFactor(), 0.001);
    }

    public void testCreateWithArgsInvalidName() {
        // Arrange
        String invalidStageName = "nonexistent";
        Map<String, Object> args = Map.of();

        // Act & Assert
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> PipelineStageFactory.createWithArgs(invalidStageName, args)
        );
        assertTrue(exception.getMessage().contains("Unknown stage type"));
        assertTrue(exception.getMessage().contains(invalidStageName));
    }

    public void testCreateWithArgsNullName() {
        // Arrange
        Map<String, Object> args = Map.of("factor", 1.0);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> PipelineStageFactory.createWithArgs(null, args)
        );
        assertTrue(exception.getMessage().contains("Stage type cannot be null"));
    }

    public void testCreateWithArgsEmptyName() {
        // Arrange
        Map<String, Object> args = Map.of("factor", 1.0);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> PipelineStageFactory.createWithArgs("", args)
        );
        assertTrue(exception.getMessage().contains("Stage type cannot be null or empty"));
    }

    public void testCreateWithArgsMissingRequiredArgs() {
        // Arrange
        String stageName = "scale";
        Map<String, Object> emptyArgs = Map.of();

        // Act & Assert
        Exception exception = expectThrows(Exception.class, () -> PipelineStageFactory.createWithArgs(stageName, emptyArgs));
        // Should throw some exception related to missing factor argument
        assertNotNull(exception);
    }

    public void testCreateWithArgsInvalidArgType() {
        // Arrange
        String stageName = "scale";
        Map<String, Object> invalidArgs = Map.of("factor", "not_a_number");

        // Act & Assert
        Exception exception = expectThrows(Exception.class, () -> PipelineStageFactory.createWithArgs(stageName, invalidArgs));
        // Should throw some exception related to invalid argument type
        assertNotNull(exception);
    }

    public void testGetSupportedStageTypes() {
        // Act
        Set<String> supportedStages = PipelineStageFactory.getSupportedStageTypes();

        // Assert
        assertNotNull(supportedStages);
        assertFalse(supportedStages.isEmpty());
        assertTrue(supportedStages.contains("scale"));
    }

    public void testIsStageTypeSupported() {
        // Test supported stage
        assertTrue(PipelineStageFactory.isStageTypeSupported("scale"));

        // Test unsupported stage
        assertFalse(PipelineStageFactory.isStageTypeSupported("nonexistent"));
    }

    public void testStageRegistration() throws Exception {
        // Test that ScaleStage is properly registered via annotation
        Set<String> supportedStages = PipelineStageFactory.getSupportedStageTypes();

        // Verify ScaleStage is registered
        assertTrue(supportedStages.contains("scale"));
        Class<? extends PipelineStage> stageClass = ScaleStage.class;

        // Verify the class has the annotation
        assertTrue(stageClass.isAnnotationPresent(PipelineStageAnnotation.class));
        PipelineStageAnnotation annotation = stageClass.getAnnotation(PipelineStageAnnotation.class);
        assertEquals("scale", annotation.name());
    }

    public void testFactoryUsesFromArgsMethod() throws Exception {
        // Arrange
        String stageName = "scale";
        Map<String, Object> args = Map.of("factor", 3.5);

        // Act
        PipelineStage stage = PipelineStageFactory.createWithArgs(stageName, args);

        // Assert
        assertNotNull(stage);
        assertTrue(stage instanceof ScaleStage);
        assertEquals(3.5, ((ScaleStage) stage).getFactor(), 0.001);
    }

    public void testFactoryReflectionMechanism() throws Exception {
        // Test that the factory correctly uses reflection to find fromArgs method
        Class<ScaleStage> stageClass = ScaleStage.class;

        // Verify fromArgs method exists
        Method fromArgsMethod = stageClass.getMethod("fromArgs", Map.class);
        assertNotNull(fromArgsMethod);
        assertTrue(java.lang.reflect.Modifier.isStatic(fromArgsMethod.getModifiers()));

        // Verify method can be invoked
        Map<String, Object> args = Map.of("factor", 1.5);
        Object result = fromArgsMethod.invoke(null, args);
        assertNotNull(result);
        assertTrue(result instanceof ScaleStage);
    }

    public void testFactoryHandlesMultipleStageTypes() {
        // Get all supported stages
        Set<String> supportedStages = PipelineStageFactory.getSupportedStageTypes();

        // Should have at least ScaleStage
        assertTrue(supportedStages.size() >= 1);
        assertTrue(supportedStages.contains("scale"));

        // Test that we can verify ScaleStage annotation
        assertTrue("ScaleStage should have annotation", ScaleStage.class.isAnnotationPresent(PipelineStageAnnotation.class));

        PipelineStageAnnotation annotation = ScaleStage.class.getAnnotation(PipelineStageAnnotation.class);
        assertEquals("Annotation name should match registry key", "scale", annotation.name());
    }

    public void testFactoryConsistency() {
        // Test that getting supported stages returns consistent results
        Set<String> stages1 = PipelineStageFactory.getSupportedStageTypes();
        Set<String> stages2 = PipelineStageFactory.getSupportedStageTypes();

        // Should return same content (though not necessarily same instance)
        assertEquals(stages1.size(), stages2.size());
        for (String key : stages1) {
            assertTrue(stages2.contains(key));
        }
    }

    public void testCreateWithArgsComplexArgs() {
        // Test factory with more complex argument scenarios
        String stageName = "scale";

        // Test with integer that should be converted to double
        Map<String, Object> intArgs = Map.of("factor", 5);
        PipelineStage intStage = PipelineStageFactory.createWithArgs(stageName, intArgs);
        assertNotNull(intStage);
        assertEquals(5.0, ((ScaleStage) intStage).getFactor(), 0.001);

        // Test with negative value
        Map<String, Object> negativeArgs = Map.of("factor", -2.5);
        PipelineStage negativeStage = PipelineStageFactory.createWithArgs(stageName, negativeArgs);
        assertNotNull(negativeStage);
        assertEquals(-2.5, ((ScaleStage) negativeStage).getFactor(), 0.001);

        // Test with zero
        Map<String, Object> zeroArgs = Map.of("factor", 0.0);
        PipelineStage zeroStage = PipelineStageFactory.createWithArgs(stageName, zeroArgs);
        assertNotNull(zeroStage);
        assertEquals(0.0, ((ScaleStage) zeroStage).getFactor(), 0.001);
    }

    public void testFactoryErrorMessages() {
        // Test that error messages are informative
        try {
            PipelineStageFactory.createWithArgs("unknown_stage", Map.of());
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            assertTrue("Error should mention stage name", e.getMessage().contains("unknown_stage"));
            assertTrue("Error should mention 'Unknown stage type'", e.getMessage().contains("Unknown stage type"));
        }

        try {
            PipelineStageFactory.createWithArgs(null, Map.of());
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            assertTrue("Error should mention null or empty", e.getMessage().contains("null") || e.getMessage().contains("empty"));
        }

        try {
            PipelineStageFactory.createWithArgs("", Map.of());
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            assertTrue("Error should mention empty", e.getMessage().contains("empty"));
        }
    }

    public void testRegisterStageType() {
        // Test manual registration (though in practice auto-registration is used)
        Set<String> originalStages = PipelineStageFactory.getSupportedStageTypes();

        // The registerStageType method allows manual registration
        // For testing, just verify our known stage is already registered
        assertTrue("Scale stage should be registered", originalStages.contains("scale"));
    }

    // ========== Registration and Exception Tests ==========

    public void testCreateWithArgsExceptionHandling() {
        // Test exception handling in createWithArgs when stage creation fails
        String stageName = "scale";

        // Test with arguments that will cause ScaleStage.fromArgs to fail
        Map<String, Object> invalidArgs = Map.of("invalid_key", "invalid_value");

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> PipelineStageFactory.createWithArgs(stageName, invalidArgs)
        );

        assertTrue("Should contain stage type in error", exception.getMessage().contains(stageName));
        assertTrue("Should indicate creation failure", exception.getMessage().contains("Failed to create stage"));
    }

    public void testConcurrentAccess() {
        // Test that factory is thread-safe
        int numThreads = 5;
        Thread[] threads = new Thread[numThreads];
        Exception[] exceptions = new Exception[numThreads];

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < 10; j++) {
                        // Test concurrent access to factory methods
                        assertTrue(
                            "isStageTypeSupported should work in thread " + threadId,
                            PipelineStageFactory.isStageTypeSupported("scale")
                        );

                        Set<String> supportedTypes = PipelineStageFactory.getSupportedStageTypes();
                        assertTrue("getSupportedStageTypes should work in thread " + threadId, supportedTypes.contains("scale"));
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

        // Wait for completion
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

    public void testFactoryStateConsistency() {
        // Test that factory state remains consistent across operations
        Set<String> initialTypes = PipelineStageFactory.getSupportedStageTypes();

        // Perform various operations
        assertTrue("scale should be supported", PipelineStageFactory.isStageTypeSupported("scale"));
        assertFalse("nonexistent should not be supported", PipelineStageFactory.isStageTypeSupported("nonexistent"));

        // Verify state consistency
        Set<String> finalTypes = PipelineStageFactory.getSupportedStageTypes();
        assertEquals("Supported types should remain consistent", initialTypes.size(), finalTypes.size());

        for (String type : initialTypes) {
            assertTrue("All initial types should still be supported", finalTypes.contains(type));
        }
    }

    public void testGetSupportedStageTypesImmutability() {
        // Test that returned set is independent of internal state
        Set<String> supportedTypes1 = PipelineStageFactory.getSupportedStageTypes();
        Set<String> supportedTypes2 = PipelineStageFactory.getSupportedStageTypes();

        // Verify they're different instances but same content
        assertNotSame("Should return different instances", supportedTypes1, supportedTypes2);
        assertEquals("Should have same content", supportedTypes1, supportedTypes2);

        // Verify modifying returned set doesn't affect factory
        String testType = "modification_test";
        supportedTypes1.add(testType);

        assertFalse("Factory should not be affected by external modification", PipelineStageFactory.isStageTypeSupported(testType));
    }

    // ========== Factory-Level Serialization Integration Tests ==========

    public void testFactoryReadFromWithScaleStage() throws Exception {
        // Test that factory's readFrom method properly calls ScaleStage.readFrom
        ScaleStage originalStage = new ScaleStage(3.5);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // First write the stage name, then the stage data (as factory expects)
            out.writeString("scale");
            originalStage.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                // Use factory's readFrom method to deserialize
                PipelineStage deserializedStage = PipelineStageFactory.readFrom(in);

                // Verify it's the correct type and has correct data
                assertNotNull("Factory readFrom should return non-null stage", deserializedStage);
                assertTrue("Factory should create ScaleStage instance", deserializedStage instanceof ScaleStage);
                assertEquals("Deserialized stage should have same factor", 3.5, ((ScaleStage) deserializedStage).getFactor(), 0.001);
                assertEquals("Deserialized stage should have same name", "scale", deserializedStage.getName());
            }
        }
    }

    public void testFactoryReadFromWithStageName() throws Exception {
        // Test the overloaded readFrom method that takes stageName parameter
        ScaleStage originalStage = new ScaleStage(2.75);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Write only the stage data (no stage name since we'll provide it separately)
            originalStage.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                // Use factory's readFrom method with explicit stage name
                PipelineStage deserializedStage = PipelineStageFactory.readFrom(in, "scale");

                // Verify it's the correct type and has correct data
                assertNotNull("Factory readFrom with stageName should return non-null stage", deserializedStage);
                assertTrue("Factory should create ScaleStage instance", deserializedStage instanceof ScaleStage);
                assertEquals("Deserialized stage should have same factor", 2.75, ((ScaleStage) deserializedStage).getFactor(), 0.001);
                assertEquals("Deserialized stage should have same name", "scale", deserializedStage.getName());
            }
        }
    }

    public void testFactoryReadFromIntegrationWithWriteTo() throws Exception {
        // Test complete serialization round-trip through factory
        ScaleStage originalStage = new ScaleStage(1.25);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Write stage name and data
            out.writeString(originalStage.getName());
            originalStage.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                // Factory should read both stage name and stage data
                PipelineStage deserializedStage = PipelineStageFactory.readFrom(in);

                // Verify complete round-trip
                assertNotNull("Round-trip should produce non-null stage", deserializedStage);
                assertTrue("Round-trip should produce ScaleStage", deserializedStage instanceof ScaleStage);
                assertEquals("Round-trip should preserve factor", 1.25, ((ScaleStage) deserializedStage).getFactor(), 0.001);
                assertEquals("Round-trip should preserve name", originalStage.getName(), deserializedStage.getName());

                // Verify behavioral equivalence
                List<TimeSeries> emptyInput = new ArrayList<>();
                List<TimeSeries> originalResult = originalStage.process(emptyInput);
                List<TimeSeries> deserializedResult = deserializedStage.process(emptyInput);
                assertEquals("Behavior should be equivalent", originalResult.size(), deserializedResult.size());
            }
        }
    }

    public void testFactoryReadFromWithInvalidStageName() throws Exception {
        // Test that factory properly handles unknown stage names
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString("unknown_stage");
            out.writeDouble(1.0); // Some dummy data

            try (StreamInput in = out.bytes().streamInput()) {
                IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> PipelineStageFactory.readFrom(in));
                assertTrue("Error should mention unknown stage", exception.getMessage().contains("Unknown stage name"));
                assertTrue("Error should mention stage name", exception.getMessage().contains("unknown_stage"));
            }
        }
    }

    public void testFactoryReadFromWithCorruptedData() throws Exception {
        // Test that factory handles corrupted serialization data properly
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString("scale");
            // Write incomplete data - not enough bytes for a double
            out.writeInt(42); // Only 4 bytes instead of 8 needed for double

            try (StreamInput in = out.bytes().streamInput()) {
                // Should throw some kind of exception when trying to read double
                Exception exception = expectThrows(Exception.class, () -> PipelineStageFactory.readFrom(in));
                assertNotNull("Should throw exception for corrupted data", exception);
            }
        }
    }

    public void testFactoryReadFromReflectionMechanism() throws Exception {
        // Test that factory correctly uses reflection to call stage-specific readFrom methods
        ScaleStage originalStage = new ScaleStage(4.5);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStage.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                // This should internally use reflection to call ScaleStage.readFrom()
                PipelineStage deserializedStage = PipelineStageFactory.readFrom(in, "scale");

                // Verify that the reflection mechanism worked correctly
                assertNotNull("Reflection-based deserialization should work", deserializedStage);
                assertTrue("Should create ScaleStage via reflection", deserializedStage instanceof ScaleStage);
                assertEquals("Reflection should preserve data", 4.5, ((ScaleStage) deserializedStage).getFactor(), 0.001);

                // Verify that it's a different instance (not cached)
                assertNotSame("Should create new instance", originalStage, deserializedStage);
            }
        }
    }
}
