/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

public class ShowTagsStageTests extends AbstractWireSerializingTestCase<ShowTagsStage> {

    public void testShowAllTagsWithKeys() {
        ShowTagsStage stage = new ShowTagsStage(true, List.of());

        List<Sample> samples = List.of(new FloatSample(10L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("name", "metric1", "city", "atlanta", "dc", "dca1");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        // Tags should be sorted alphabetically: city, dc, name
        assertEquals("city:atlanta dc:dca1 name:metric1", result.get(0).getAlias());
    }

    public void testShowAllTagsWithoutKeys() {
        ShowTagsStage stage = new ShowTagsStage(false, List.of());

        List<Sample> samples = List.of(new FloatSample(10L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("name", "metric1", "city", "atlanta", "dc", "dca1");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        // Values only, sorted by key alphabetically: city, dc, name
        assertEquals("atlanta dca1 metric1", result.get(0).getAlias());
    }

    public void testShowSpecificTagsWithKeys() {
        ShowTagsStage stage = new ShowTagsStage(true, List.of("city", "dc"));

        List<Sample> samples = List.of(new FloatSample(10L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("name", "metric1", "city", "atlanta", "dc", "dca1");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        // Tags in specified order: city, dc
        assertEquals("city:atlanta dc:dca1", result.get(0).getAlias());
    }

    public void testShowSpecificTagsWithoutKeys() {
        ShowTagsStage stage = new ShowTagsStage(false, List.of("dc", "city"));

        List<Sample> samples = List.of(new FloatSample(10L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("name", "metric1", "city", "atlanta", "dc", "dca1");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        // Values in specified order: dc, city
        assertEquals("dca1 atlanta", result.get(0).getAlias());
    }

    public void testSkipMissingTags() {
        ShowTagsStage stage = new ShowTagsStage(true, List.of("city", "missing", "dc"));

        List<Sample> samples = List.of(new FloatSample(10L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("name", "metric1", "city", "atlanta", "dc", "dca1");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        // Only existing tags shown: city, dc (missing skipped)
        assertEquals("city:atlanta dc:dca1", result.get(0).getAlias());
    }

    public void testAllTagsMissing() {
        ShowTagsStage stage = new ShowTagsStage(true, List.of("missing1", "missing2"));

        List<Sample> samples = List.of(new FloatSample(10L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("name", "metric1", "city", "atlanta");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        // No matching tags, empty alias
        assertEquals("", result.get(0).getAlias());
    }

    public void testEmptyLabels() {
        ShowTagsStage stage = new ShowTagsStage(true, List.of());

        List<Sample> samples = List.of(new FloatSample(10L, 10.0));
        ByteLabels labels = ByteLabels.emptyLabels();
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("", result.get(0).getAlias());
    }

    public void testMultipleSeries() {
        ShowTagsStage stage = new ShowTagsStage(true, List.of("city"));

        List<Sample> samples1 = List.of(new FloatSample(10L, 10.0));
        List<Sample> samples2 = List.of(new FloatSample(20L, 20.0));

        ByteLabels labels1 = ByteLabels.fromStrings("city", "atlanta", "name", "metric1");
        ByteLabels labels2 = ByteLabels.fromStrings("city", "boston", "name", "metric2");

        TimeSeries ts1 = new TimeSeries(samples1, labels1, 10L, 10L, 10L, null);
        TimeSeries ts2 = new TimeSeries(samples2, labels2, 20L, 20L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(ts1, ts2));

        assertEquals(2, result.size());
        assertEquals("city:atlanta", result.get(0).getAlias());
        assertEquals("city:boston", result.get(1).getAlias());
    }

    public void testSingleTagOnly() {
        ShowTagsStage stage = new ShowTagsStage(false, List.of("city"));

        List<Sample> samples = List.of(new FloatSample(10L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("city", "atlanta");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("atlanta", result.get(0).getAlias());
    }

    public void testWithEmptyInput() {
        ShowTagsStage stage = new ShowTagsStage(true, List.of("city"));
        List<TimeSeries> result = stage.process(List.of());
        assertTrue(result.isEmpty());
    }

    public void testGetName() {
        ShowTagsStage stage = new ShowTagsStage(true, List.of("city"));
        assertEquals("show_tags", stage.getName());
    }

    public void testNullInputThrowsException() {
        ShowTagsStage stage = new ShowTagsStage(true, List.of("city"));
        TestUtils.assertNullInputThrowsException(stage, "show_tags");
    }

    public void testSupportConcurrentSegmentSearch() {
        ShowTagsStage stage = new ShowTagsStage(true, List.of("city"));
        assertTrue(stage.supportConcurrentSegmentSearch());
    }

    public void testFromArgs() {
        Map<String, Object> args = Map.of("show_keys", true, "tags", List.of("city", "dc"));
        ShowTagsStage stage = ShowTagsStage.fromArgs(args);

        List<Sample> samples = List.of(new FloatSample(10L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("city", "atlanta", "dc", "dca1");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));
        assertEquals("city:atlanta dc:dca1", result.get(0).getAlias());
    }

    public void testFromArgsWithoutKeys() {
        Map<String, Object> args = Map.of("show_keys", false, "tags", List.of("city"));
        ShowTagsStage stage = ShowTagsStage.fromArgs(args);

        List<Sample> samples = List.of(new FloatSample(10L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("city", "atlanta");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));
        assertEquals("atlanta", result.get(0).getAlias());
    }

    public void testFromArgsEmptyTags() {
        Map<String, Object> args = Map.of("show_keys", true, "tags", List.of());
        ShowTagsStage stage = ShowTagsStage.fromArgs(args);

        List<Sample> samples = List.of(new FloatSample(10L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("b", "beta", "a", "alpha");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));
        // Should be sorted alphabetically
        assertEquals("a:alpha b:beta", result.get(0).getAlias());
    }

    public void testFromArgsMissingShowKeys() {
        Map<String, Object> args = Map.of("tags", List.of("city"));

        Exception exception = assertThrows(IllegalArgumentException.class, () -> { ShowTagsStage.fromArgs(args); });

        assertTrue(exception.getMessage().contains("show_keys"));
    }

    public void testFromArgsMissingTags() {
        Map<String, Object> args = Map.of("show_keys", true);

        Exception exception = assertThrows(IllegalArgumentException.class, () -> { ShowTagsStage.fromArgs(args); });

        assertTrue(exception.getMessage().contains("tags"));
    }

    public void testPipelineStageFactory() {
        assertTrue(PipelineStageFactory.isStageTypeSupported(ShowTagsStage.NAME));

        PipelineStage stage = PipelineStageFactory.createWithArgs(ShowTagsStage.NAME, Map.of("show_keys", false, "tags", List.of("city")));

        assertTrue(stage instanceof ShowTagsStage);
    }

    public void testToXContent() throws IOException {
        ShowTagsStage stage = new ShowTagsStage(true, List.of("city", "dc"));

        try (XContentBuilder builder = XContentFactory.jsonBuilder().startObject()) {
            stage.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();

            String json = builder.toString();
            assertEquals("{\"show_keys\":true,\"tags\":[\"city\",\"dc\"]}", json);
        }
    }

    @Override
    protected ShowTagsStage createTestInstance() {
        boolean showKeys = randomBoolean();
        int numTags = randomIntBetween(0, 5);
        List<String> tags = new ArrayList<>();
        for (int i = 0; i < numTags; i++) {
            tags.add(randomAlphaOfLength(5));
        }
        return new ShowTagsStage(showKeys, tags);
    }

    @Override
    protected Writeable.Reader<ShowTagsStage> instanceReader() {
        return ShowTagsStage::new;
    }
}
