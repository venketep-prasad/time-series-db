/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.tsdb.core.model.ByteLabels;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * JMH Benchmark for ByteLabels operations.
 * Measures average latency for equals, hashCode, and toString methods.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class LabelsBenchmark {

    private ByteLabels initialized10Labels;
    private ByteLabels initialized40Labels;
    private ByteLabels labels10Labels;
    private ByteLabels labels40Labels;
    public final ByteLabels STATIC_LABELS = ByteLabels.fromMap(LABEL_OF_10_STRING);
    final static Map<String, String> LABELS = Map.of(
        "__name__",
        "cpu_usage",
        "host",
        "server1",
        "region",
        "us-east-1",
        "env",
        "production",
        "team",
        "platform"
    );
    final static Map<String, String> LABEL_OF_10_STRING = IntStream.rangeClosed(1, 10)
        .boxed()
        .collect(
            Collectors.toMap(
                // Key Mapper: Function to generate the map key (e.g., 1 -> "key_1")
                i -> "key_" + i,
                // Value Mapper: Function to generate the map value (e.g., 1 -> "value_1")
                i -> "value_" + i
            )
        );

    final static Map<String, String> LABEL_OF_40_STRING = IntStream.rangeClosed(1, 40)
        .boxed()
        .collect(
            Collectors.toMap(
                // Key Mapper: Function to generate the map key (e.g., 1 -> "key_1")
                i -> "key_" + i,
                // Value Mapper: Function to generate the map value (e.g., 1 -> "value_1")
                i -> "value_" + i
            )
        );

    /**
     * Setup method to initialize ByteLabels instances for benchmarking.
     */
    @Setup(Level.Trial)
    public void setup() {
        // initialize hashcode cache
        STATIC_LABELS.hashCode();
        labels10Labels = ByteLabels.fromMap(LABELS);
        labels40Labels = ByteLabels.fromMap(LABEL_OF_40_STRING);
    }

    @Setup(Level.Invocation)
    public void setupPerInvocation() {
        initialized10Labels = ByteLabels.fromMap(LABEL_OF_10_STRING);
        initialized40Labels = ByteLabels.fromMap(LABEL_OF_40_STRING);
    }

    /**
     * Benchmark for ByteLabels.equals() method with equal labels.
     */
    @Benchmark
    public void benchmarkEqualsTrue(LabelsBenchmark state, Blackhole bh) {
        bh.consume(initialized10Labels.equals(state.STATIC_LABELS));
    }

    /**
     * Benchmark for ByteLabels.equals() method with different labels.
     */
    @Benchmark
    public void benchmarkEqualsFalse(Blackhole bh) {
        bh.consume(labels10Labels.equals(initialized10Labels));
    }

    /**
     * Benchmark for ByteLabels.hashCode() method.
     */
    @Benchmark
    public void benchmark10LabelsHashCode(Blackhole bh) {
        bh.consume(initialized10Labels.hashCode());
    }

    @Benchmark
    public void benchmark40LabelsHashCode(Blackhole bh) {
        bh.consume(initialized40Labels.hashCode());
    }

    @Benchmark
    public void benchmark10LabelsHashCodeCached(LabelsBenchmark state, Blackhole bh) {
        bh.consume(state.STATIC_LABELS.hashCode());
    }

    /**
     * Benchmark for ByteLabels.toString() method.
     */
    @Benchmark
    public void benchmark10LabelsToString(Blackhole bh) {
        bh.consume(initialized10Labels.toString());
    }

    @Benchmark
    public void benchmark40LabelsToString(Blackhole bh) {
        bh.consume(initialized40Labels.toString());
    }

    @Benchmark
    public void benchmark10LabelsToStringStatic(LabelsBenchmark state, Blackhole bh) {
        bh.consume(state.STATIC_LABELS.toString());
    }
}
