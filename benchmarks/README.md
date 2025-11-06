# OpenSearch TSDB Benchmarks

This module contains JMH (Java Microbenchmark Harness) benchmarks for performance testing critical components of the OpenSearch TSDB plugin.

## Prerequisites

- Java 21
- Gradle 6.5.1+

## Running Benchmarks

### Run All Benchmarks

```bash
./gradlew :benchmarks:jmh
```

### Run Specific Benchmark

```bash
# Run only MergeIteratorBenchmark
./gradlew :benchmarks:jmh -Pjmh.includes="MergeIteratorBenchmark"

# Run specific method
./gradlew :benchmarks:jmh -Pjmh.includes="MergeIteratorBenchmark.benchmarkHeapMerge"
```

### Run with Profiling

```bash
# Run with JFR profiler (shows cpu profiles by default)
./gradlew :benchmarks:jmh -Pjmh.includes="MergeIteratorBenchmark" -Pjmh.profilers=jfr

# Run with GC profiler (shows allocations and GC overhead)
./gradlew :benchmarks:jmh -Pjmh.includes="MergeIteratorBenchmark" -Pjmh.profilers=gc

# Run with stack profiler (shows method hotspots)
./gradlew :benchmarks:jmh -Pjmh.includes="MergeIteratorBenchmark" -Pjmh.profilers=stack

# Run with multiple profilers
./gradlew :benchmarks:jmh -Pjmh.includes="MergeIteratorBenchmark" -Pjmh.profilers=gc,stack
```

### Customize Benchmark Parameters

```bash
# Adjust warmup and measurement iterations
./gradlew :benchmarks:jmh -Pjmh.includes="MergeIteratorBenchmark" \
  -Pjmh.warmupIterations=5 \
  -Pjmh.measurementIterations=10

# Adjust number of forks
./gradlew :benchmarks:jmh -Pjmh.includes="MergeIteratorBenchmark" \
  -Pjmh.forks=3
```

## Utility Tasks

```bash
# List all available benchmarks
./gradlew :benchmarks:jmhList

# List all available profilers
./gradlew :benchmarks:jmhProfilers

# Show JMH help
./gradlew :benchmarks:jmhHelp
```

## Adding New Benchmarks

~~1. Create a new class in `src/main/java/org/opensearch/tsdb/benchmark/`
2. Add JMH annotations:
   ```java
   @State(Scope.Benchmark)
   @BenchmarkMode(Mode.Throughput)
   @OutputTimeUnit(TimeUnit.MICROSECONDS)
   public class MyBenchmark {
       @Benchmark
       public void benchmarkMethod(Blackhole bh) {
           // Your benchmark code
       }
   }
   ```
3. Run with `./gradlew :benchmarks:jmh -Pjmh.includes="MyBenchmark"`~~

## Interpreting Results

### Throughput Mode
Higher is better. Measures operations per time unit.

```
Benchmark                                  Mode  Cnt    Score   Error  Units
MergeIteratorBenchmark.benchmarkHeapMerge  thrpt   5  1234.5 ± 10.2  ops/ms
```

### Average Time Mode
Lower is better. Measures time per operation.

```
Benchmark                                  Mode  Cnt    Score   Error  Units
MergeIteratorBenchmark.benchmarkHeapMerge  avgt   5   0.810 ± 0.007  ms/op
```

### GC Profiler Output
```
·gc.alloc.rate          thrpt   5   456.7 ±  5.3  MB/sec    # Allocation rate
·gc.alloc.rate.norm     thrpt   5  4096.0 ±  0.0    B/op    # Bytes per operation
·gc.count               thrpt   5    10.0          counts   # GC count
```

## References

- [JMH Documentation](https://github.com/openjdk/jmh)
- [JMH Samples](https://github.com/openjdk/jmh/tree/master/jmh-samples/src/main/java/org/openjdk/jmh/samples)
