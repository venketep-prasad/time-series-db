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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.lang.m3.stage.AbstractGroupingSampleStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.ParallelProcessingConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark to find the crossover point where parallel processing in grouping stages
 * outperforms sequential processing.
 *
 * <p>Tests across a matrix of (seriesCount, samplesPerSeries) to measure both the
 * processGroup (coordinator aggregation) and reduceGrouped (shard merge) paths.</p>
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :benchmarks:jmh -Pjmh.includes="GroupingStageParallelBenchmark"
 * </pre>
 * Or for a specific method:
 * <pre>
 *   ./gradlew :benchmarks:jmh -Pjmh.includes="GroupingStageParallelBenchmark.processSequential"
 * </pre>
 * </p>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
public class GroupingStageParallelBenchmark {

    // --- Parameterized axes: series count x samples per series ---

    @Param({"100", "500", "1000", "5000"})
    private int seriesCount;

    @Param({"100", "500", "1000", "5000"})
    private int samplesPerSeries;

    // --- Pre-built data (created once per trial) ---

    /** Input for processGroup benchmarks: flat list of time series (no grouping). */
    private List<TimeSeries> processInput;

    /** Input for reduceGrouped benchmarks: list of aggregation providers (simulating shards). */
    private List<TimeSeriesProvider> reduceInput;

    /** First aggregation (metadata source for reduce). */
    private TimeSeriesProvider firstAgg;

    /** First time series (metadata source for reduce). */
    private TimeSeries firstTimeSeries;

    private final SumStage sequentialStage = new SumStage();
    private final SumStage parallelStage = new SumStage();

    @Setup(Level.Trial)
    public void setup() {
        // Build processGroup input
        processInput = buildDataset(seriesCount, samplesPerSeries, Map.of());

        // Build reduceGrouped input: split data across 10 simulated shards
        int shardsCount = 10;
        int seriesPerShard = Math.max(1, seriesCount / shardsCount);
        List<TimeSeriesProvider> providers = new ArrayList<>(shardsCount);
        for (int shard = 0; shard < shardsCount; shard++) {
            int start = shard * seriesPerShard;
            int end = (shard == shardsCount - 1) ? seriesCount : Math.min(start + seriesPerShard, seriesCount);
            if (start >= seriesCount) break;
            List<TimeSeries> shardData = buildDataset(end - start, samplesPerSeries, Map.of(), start);
            providers.add(new InternalTimeSeries("shard" + shard, shardData, Map.of()));
        }
        reduceInput = providers;
        firstAgg = providers.get(0);
        firstTimeSeries = providers.get(0).getTimeSeries().get(0);
    }

    // ==================== processGroup benchmarks ====================

    @Benchmark
    public void processSequential(Blackhole bh) {
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.sequentialOnly());
        bh.consume(sequentialStage.process(processInput));
    }

    @Benchmark
    public void processParallel(Blackhole bh) {
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.alwaysParallel());
        bh.consume(parallelStage.process(processInput));
    }

    // ==================== reduceGrouped benchmarks ====================

    @Benchmark
    public void reduceSequential(Blackhole bh) {
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.sequentialOnly());
        bh.consume(sequentialStage.reduce(reduceInput, true));
    }

    @Benchmark
    public void reduceParallel(Blackhole bh) {
        AbstractGroupingSampleStage.setParallelConfig(ParallelProcessingConfig.alwaysParallel());
        bh.consume(parallelStage.reduce(reduceInput, true));
    }

    // ==================== Data builders ====================

    private static List<TimeSeries> buildDataset(int numSeries, int samplesPerSeries, Map<String, String> labels) {
        return buildDataset(numSeries, samplesPerSeries, labels, 0);
    }

    private static List<TimeSeries> buildDataset(
        int numSeries,
        int samplesPerSeries,
        Map<String, String> labels,
        int seriesOffset
    ) {
        Labels labelObj = labels.isEmpty() ? ByteLabels.emptyLabels() : ByteLabels.fromMap(labels);
        List<TimeSeries> result = new ArrayList<>(numSeries);
        for (int s = 0; s < numSeries; s++) {
            FloatSampleList.Builder builder = new FloatSampleList.Builder();
            for (int v = 0; v < samplesPerSeries; v++) {
                builder.add(1000L + v * 1000L, (double) ((seriesOffset + s) * samplesPerSeries + v));
            }
            long maxTs = 1000L + (samplesPerSeries - 1) * 1000L;
            result.add(new TimeSeries(builder.build(), labelObj, 1000L, maxTs, 1000L, "ts_" + (seriesOffset + s)));
        }
        return result;
    }
}
