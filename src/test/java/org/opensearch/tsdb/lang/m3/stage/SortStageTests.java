/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SortStageTests extends AbstractWireSerializingTestCase<SortStage> {

    // ========== Constructor Tests ==========

    public void testConstructorWithBothParameters() {
        // Arrange & Act
        SortStage sortStage = new SortStage(SortStage.SortBy.AVG, SortStage.SortOrder.ASC);

        // Assert
        assertEquals(SortStage.SortBy.AVG, sortStage.getSortBy());
        assertEquals(SortStage.SortOrder.ASC, sortStage.getSortOrder());
        assertEquals("sort", sortStage.getName());
    }

    public void testConstructorWithSortByOnly() {
        // Arrange & Act
        SortStage sortStage = new SortStage(SortStage.SortBy.MAX);

        // Assert
        assertEquals(SortStage.SortBy.MAX, sortStage.getSortBy());
        assertEquals(SortStage.SortOrder.DESC, sortStage.getSortOrder()); // Default
        assertEquals("sort", sortStage.getName());
    }

    // ========== Process Method Tests ==========

    public void testProcessWithEmptyInput() {
        // Arrange
        SortStage sortStage = new SortStage(SortStage.SortBy.AVG);
        List<TimeSeries> input = new ArrayList<>();

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert
        assertSame(input, result); // Should return the same empty list
    }

    public void testProcessWithNullInput() {
        // Arrange
        SortStage sortStage = new SortStage(SortStage.SortBy.AVG);

        // Act & Assert
        NullPointerException exception = expectThrows(NullPointerException.class, () -> sortStage.process(null));
        assertEquals("Input cannot be null", exception.getMessage());
    }

    public void testProcessSortByAvgDesc() {
        // Arrange
        SortStage sortStage = new SortStage(SortStage.SortBy.AVG, SortStage.SortOrder.DESC);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert
        assertEquals(3, result.size());
        // Should be sorted by average in descending order: [2.0, 1.5, 1.0]
        assertEquals(2.0, getAverage(result.get(0)), 0.001);
        assertEquals(1.5, getAverage(result.get(1)), 0.001);
        assertEquals(1.0, getAverage(result.get(2)), 0.001);
    }

    public void testProcessSortByAvgAsc() {
        // Arrange
        SortStage sortStage = new SortStage(SortStage.SortBy.AVG, SortStage.SortOrder.ASC);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert
        assertEquals(3, result.size());
        // Should be sorted by average in ascending order: [1.0, 1.5, 2.0]
        assertEquals(1.0, getAverage(result.get(0)), 0.001);
        assertEquals(1.5, getAverage(result.get(1)), 0.001);
        assertEquals(2.0, getAverage(result.get(2)), 0.001);
    }

    public void testProcessSortByMaxDesc() {
        // Arrange
        SortStage sortStage = new SortStage(SortStage.SortBy.MAX, SortStage.SortOrder.DESC);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert
        assertEquals(3, result.size());
        // Should be sorted by max in descending order: [3.0, 2.0, 1.0]
        assertEquals(3.0, getMax(result.get(0)), 0.001);
        assertEquals(2.0, getMax(result.get(1)), 0.001);
        assertEquals(1.0, getMax(result.get(2)), 0.001);
    }

    public void testProcessSortBySumDesc() {
        // Arrange
        SortStage sortStage = new SortStage(SortStage.SortBy.SUM, SortStage.SortOrder.DESC);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert
        assertEquals(3, result.size());
        // Should be sorted by sum in descending order: [6.0, 3.0, 2.0]
        assertEquals(6.0, getSum(result.get(0)), 0.001);
        assertEquals(3.0, getSum(result.get(1)), 0.001);
        assertEquals(2.0, getSum(result.get(2)), 0.001);
    }

    public void testProcessWithSingleTimeSeries() {
        // Arrange
        SortStage sortStage = new SortStage(SortStage.SortBy.AVG);
        List<TimeSeries> input = Arrays.asList(createTimeSeries(Arrays.asList(1.0, 2.0, 3.0)));

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertSame(input.get(0), result.get(0)); // Should return the same time series
    }

    public void testProcessWithEmptyTimeSeries() {
        // Arrange
        SortStage sortStage = new SortStage(SortStage.SortBy.AVG);
        List<TimeSeries> input = Arrays.asList(createTimeSeries(new ArrayList<>()));

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(0.0, getAverage(result.get(0)), 0.001);
    }

    public void testProcessWithNaNValues() {
        // Arrange
        SortStage sortStage = new SortStage(SortStage.SortBy.AVG);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, Double.NaN), new FloatSample(3000L, 3.0));
        Labels labels = ByteLabels.fromMap(Map.of("label", "test"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(2.0, getAverage(result.get(0)), 0.001); // Should ignore NaN values
    }

    public void testProcessWithNullSamples() {
        // Arrange
        SortStage sortStage = new SortStage(SortStage.SortBy.AVG);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), null, new FloatSample(3000L, 3.0));
        Labels labels = ByteLabels.fromMap(Map.of("label", "test"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(2.0, getAverage(result.get(0)), 0.001); // Should ignore null samples
    }

    // ========== SortBy Enum Tests ==========

    public void testSortByFromString() {
        assertEquals(SortStage.SortBy.AVG, SortStage.SortBy.fromString("avg"));
        assertEquals(SortStage.SortBy.AVG, SortStage.SortBy.fromString("AVG"));
        assertEquals(SortStage.SortBy.AVG, SortStage.SortBy.fromString("Avg"));
        assertEquals(SortStage.SortBy.MAX, SortStage.SortBy.fromString("max"));
        assertEquals(SortStage.SortBy.SUM, SortStage.SortBy.fromString("sum"));
    }

    public void testSortByFromStringInvalid() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortStage.SortBy.fromString("invalid"));
        assertTrue(exception.getMessage().contains("Unknown sort function"));
        assertTrue(exception.getMessage().contains("avg, max, sum"));
    }

    public void testSortByGetName() {
        assertEquals("avg", SortStage.SortBy.AVG.getName());
        assertEquals("max", SortStage.SortBy.MAX.getName());
        assertEquals("sum", SortStage.SortBy.SUM.getName());
    }

    // ========== SortOrder Enum Tests ==========

    public void testSortOrderFromString() {
        assertEquals(SortStage.SortOrder.ASC, SortStage.SortOrder.fromString("asc"));
        assertEquals(SortStage.SortOrder.ASC, SortStage.SortOrder.fromString("ASC"));
        assertEquals(SortStage.SortOrder.ASC, SortStage.SortOrder.fromString("Asc"));
        assertEquals(SortStage.SortOrder.DESC, SortStage.SortOrder.fromString("desc"));
    }

    public void testSortOrderFromStringInvalid() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortStage.SortOrder.fromString("invalid"));
        assertTrue(exception.getMessage().contains("Unknown sort order"));
        assertTrue(exception.getMessage().contains("asc, desc"));
    }

    public void testSortOrderGetName() {
        assertEquals("asc", SortStage.SortOrder.ASC.getName());
        assertEquals("desc", SortStage.SortOrder.DESC.getName());
    }

    // ========== Serialization Tests ==========

    public void testWriteToAndReadFrom() throws IOException {
        // Arrange
        SortStage original = new SortStage(SortStage.SortBy.MAX, SortStage.SortOrder.ASC);
        BytesStreamOutput output = new BytesStreamOutput();

        // Act
        original.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        SortStage deserialized = SortStage.readFrom(input);

        // Assert
        assertEquals(original.getSortBy(), deserialized.getSortBy());
        assertEquals(original.getSortOrder(), deserialized.getSortOrder());
    }

    public void testToXContent() throws IOException {
        // Test XContent serialization
        SortStage stage = new SortStage(SortStage.SortBy.SUM, SortStage.SortOrder.DESC);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertTrue("JSON should contain sortBy field", json.contains(SortStage.SORT_BY_ARG));
        assertTrue("JSON should contain sortOrder field", json.contains(SortStage.SORT_ORDER_ARG));
        assertTrue("JSON should contain sum", json.contains("sum"));
        assertTrue("JSON should contain desc", json.contains("desc"));
    }

    // ========== FromArgs Tests ==========

    public void testFromArgsValid() {
        // Arrange
        Map<String, Object> args = Map.of(SortStage.SORT_BY_ARG, "avg", SortStage.SORT_ORDER_ARG, "asc");

        // Act
        SortStage sortStage = SortStage.fromArgs(args);

        // Assert
        assertEquals(SortStage.SortBy.AVG, sortStage.getSortBy());
        assertEquals(SortStage.SortOrder.ASC, sortStage.getSortOrder());
    }

    public void testFromArgsWithDefaultOrder() {
        // Arrange
        Map<String, Object> args = Map.of(SortStage.SORT_BY_ARG, "max");

        // Act
        SortStage sortStage = SortStage.fromArgs(args);

        // Assert
        assertEquals(SortStage.SortBy.MAX, sortStage.getSortBy());
        assertEquals(SortStage.SortOrder.DESC, sortStage.getSortOrder()); // Default
    }

    public void testFromArgsMissingSortBy() {
        // Arrange
        Map<String, Object> args = Map.of(SortStage.SORT_ORDER_ARG, "asc");

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Sort stage requires '" + SortStage.SORT_BY_ARG + "' argument"));
    }

    public void testFromArgsNullArgs() {
        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortStage.fromArgs(null));
        assertTrue(exception.getMessage().contains("Sort stage requires '" + SortStage.SORT_BY_ARG + "' argument"));
    }

    public void testFromArgsNullSortBy() {
        // Arrange
        Map<String, Object> args = new HashMap<>();
        args.put(SortStage.SORT_BY_ARG, null);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortStage.fromArgs(args));
        assertEquals("SortBy cannot be null", exception.getMessage());
    }

    public void testFromArgsInvalidSortByType() {
        // Arrange
        Map<String, Object> args = Map.of(SortStage.SORT_BY_ARG, 123);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Invalid type for '" + SortStage.SORT_BY_ARG + "' argument"));
    }

    public void testFromArgsInvalidSortOrderType() {
        // Arrange
        Map<String, Object> args = Map.of(SortStage.SORT_BY_ARG, "avg", SortStage.SORT_ORDER_ARG, 123);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Invalid type for '" + SortStage.SORT_ORDER_ARG + "' argument"));
    }

    public void testFromArgsNullSortOrder() {
        // Arrange
        Map<String, Object> args = new HashMap<>();
        args.put(SortStage.SORT_BY_ARG, "avg");
        args.put(SortStage.SORT_ORDER_ARG, null);

        // Act
        SortStage sortStage = SortStage.fromArgs(args);

        // Assert - should use default order when null
        assertEquals(SortStage.SortBy.AVG, sortStage.getSortBy());
        assertEquals(SortStage.SortOrder.DESC, sortStage.getSortOrder());
    }

    // ========== Interface Compliance Tests ==========

    public void testIsGlobalAggregation() {
        SortStage sortStage = new SortStage(SortStage.SortBy.AVG);
        assertTrue(sortStage.isGlobalAggregation());
    }

    public void testIsCoordinatorOnly() {
        SortStage sortStage = new SortStage(SortStage.SortBy.AVG);
        assertTrue(sortStage.isCoordinatorOnly());
    }

    public void testSupportConcurrentSegmentSearch() {
        SortStage sortStage = new SortStage(SortStage.SortBy.AVG);
        assertTrue(sortStage.supportConcurrentSegmentSearch());
    }

    // ========== Edge Cases and Error Handling ==========

    public void testProcessWithVeryLargeValues() {
        // Test with very large values
        SortStage sortStage = new SortStage(SortStage.SortBy.MAX);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, Double.MAX_VALUE), new FloatSample(2000L, Double.MIN_VALUE));
        Labels labels = ByteLabels.fromMap(Map.of("label", "test"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "test");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(Double.MAX_VALUE, getMax(result.get(0)), 0.001);
    }

    public void testProcessWithAllNaNValues() {
        // Test with all NaN values
        SortStage sortStage = new SortStage(SortStage.SortBy.AVG);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, Double.NaN), new FloatSample(2000L, Double.NaN));
        Labels labels = ByteLabels.fromMap(Map.of("label", "test"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "test");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(0.0, getAverage(result.get(0)), 0.001); // Should return 0.0 for empty valid samples
    }

    public void testProcessWithAllNullSamples() {
        // Test with all null samples
        SortStage sortStage = new SortStage(SortStage.SortBy.AVG);
        List<Sample> samples = Arrays.asList(null, null);
        Labels labels = ByteLabels.fromMap(Map.of("label", "test"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "test");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(0.0, getAverage(result.get(0)), 0.001); // Should return 0.0 for empty valid samples
    }

    // ========== Helper Methods ==========

    private List<TimeSeries> createTestTimeSeries() {
        // Create time series with different averages, maxes, and sums
        // Series 1: avg=1.0, max=1.0, sum=2.0
        TimeSeries series1 = createTimeSeries(Arrays.asList(1.0, 1.0));

        // Series 2: avg=2.0, max=3.0, sum=6.0
        TimeSeries series2 = createTimeSeries(Arrays.asList(1.0, 2.0, 3.0));

        // Series 3: avg=1.5, max=2.0, sum=3.0
        TimeSeries series3 = createTimeSeries(Arrays.asList(1.0, 2.0));

        return Arrays.asList(series1, series2, series3);
    }

    private TimeSeries createTimeSeries(List<Double> values) {
        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            samples.add(new FloatSample(1000L + i * 1000L, values.get(i)));
        }
        Labels labels = ByteLabels.fromMap(Map.of("label", "test"));
        return new TimeSeries(samples, labels, 1000L, 1000L + (values.size() - 1) * 1000L, 1000L, "test");
    }

    private double getAverage(TimeSeries timeSeries) {
        List<Sample> samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return 0.0;
        }

        double sum = 0.0;
        int count = 0;
        for (Sample sample : samples) {
            if (sample != null && !Double.isNaN(sample.getValue())) {
                sum += sample.getValue();
                count++;
            }
        }

        return count == 0 ? 0.0 : sum / count;
    }

    private double getMax(TimeSeries timeSeries) {
        List<Sample> samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return Double.NEGATIVE_INFINITY;
        }

        double max = Double.NEGATIVE_INFINITY;
        for (Sample sample : samples) {
            if (sample != null && !Double.isNaN(sample.getValue())) {
                max = Math.max(max, sample.getValue());
            }
        }

        return max == Double.NEGATIVE_INFINITY ? 0.0 : max;
    }

    private double getSum(TimeSeries timeSeries) {
        List<Sample> samples = timeSeries.getSamples();
        if (samples.isEmpty()) {
            return 0.0;
        }

        double sum = 0.0;
        for (Sample sample : samples) {
            if (sample != null && !Double.isNaN(sample.getValue())) {
                sum += sample.getValue();
            }
        }

        return sum;
    }

    /**
     * Test equals method for SortStage.
     */
    public void testEquals() {
        SortStage stage1 = new SortStage(SortStage.SortBy.AVG, SortStage.SortOrder.DESC);
        SortStage stage2 = new SortStage(SortStage.SortBy.AVG, SortStage.SortOrder.DESC);

        assertEquals("Equal SortStages should be equal", stage1, stage2);

        SortStage stageDiffSortBy = new SortStage(SortStage.SortBy.MAX, SortStage.SortOrder.DESC);
        assertNotEquals("Different sortBy should not be equal", stage1, stageDiffSortBy);

        SortStage stageDiffSortOrder = new SortStage(SortStage.SortBy.AVG, SortStage.SortOrder.ASC);
        assertNotEquals("Different sortOrder should not be equal", stage1, stageDiffSortOrder);

        assertEquals("Stage should equal itself", stage1, stage1);

        assertNotEquals("Stage should not equal null", null, stage1);

        assertNotEquals("Stage should not equal different class", "string", stage1);

        for (SortStage.SortBy sortBy : SortStage.SortBy.values()) {
            for (SortStage.SortOrder sortOrder : SortStage.SortOrder.values()) {
                SortStage stage3 = new SortStage(sortBy, sortOrder);
                SortStage stage4 = new SortStage(sortBy, sortOrder);
                assertEquals("Stages with same sortBy " + sortBy + " and sortOrder " + sortOrder + " should be equal", stage3, stage4);
            }
        }

        SortStage stageDefault1 = new SortStage(SortStage.SortBy.SUM);
        SortStage stageDefault2 = new SortStage(SortStage.SortBy.SUM);
        assertEquals("Stages with default sort order should be equal", stageDefault1, stageDefault2);
    }

    @Override
    protected Writeable.Reader<SortStage> instanceReader() {
        return SortStage::readFrom;
    }

    @Override
    protected SortStage createTestInstance() {
        return new SortStage(randomFrom(SortStage.SortBy.values()), randomFrom(SortStage.SortOrder.values()));
    }
}
