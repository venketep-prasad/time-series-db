/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.TSDBDocValues;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class IndexUtilsTests extends OpenSearchTestCase {

    public void testLabelsForDocWithValidLabels() throws IOException {

        Labels expectedLabels = ByteLabels.fromStrings("__name__", "cpu_usage", "host", "server1", "region", "us-west");
        List<String> expectedIndexSet = expectedLabels.toIndexSet().stream().toList();
        MockSortedSetDocValues labelsDocValues = new MockSortedSetDocValues(expectedIndexSet);

        MockTSDBDocValues tsdbDocValues = new MockTSDBDocValues(labelsDocValues);

        Labels labels = IndexUtils.labelsForDoc(0, tsdbDocValues);

        assertNotNull("Labels should not be null", labels);
        assertEquals("Labels should contain expected labels", expectedLabels, labels);
    }

    public void testLabelsForDocWithEmptyLabels() throws IOException {
        MockSortedSetDocValues labelsDocValues = new MockSortedSetDocValues(Arrays.asList());
        MockTSDBDocValues tsdbDocValues = new MockTSDBDocValues(labelsDocValues);

        Labels labels = IndexUtils.labelsForDoc(0, tsdbDocValues);

        assertNotNull("Labels should not be null", labels);
        assertTrue("Labels should be empty", labels.isEmpty());
        assertEquals("Should be same as empty labels", ByteLabels.emptyLabels().stableHash(), labels.stableHash());
    }

    public void testLabelsForDocWithNullDocValues() throws IOException {
        MockTSDBDocValues tsdbDocValues = new MockTSDBDocValues(null);

        Labels labels = IndexUtils.labelsForDoc(0, tsdbDocValues);

        assertNotNull("Labels should not be null", labels);
        assertTrue("Labels should be empty when doc values is null", labels.isEmpty());
    }

    public void testLabelsForDocWithDocumentNotFound() throws IOException {
        MockSortedSetDocValues labelsDocValues = new MockSortedSetDocValues(Arrays.asList("test:value")) {
            @Override
            public boolean advanceExact(int target) throws IOException {
                return false; // Document not found
            }
        };

        MockTSDBDocValues tsdbDocValues = new MockTSDBDocValues(labelsDocValues);

        Labels labels = IndexUtils.labelsForDoc(999, tsdbDocValues);

        assertNotNull("Labels should not be null", labels);
        assertTrue("Labels should be empty when document not found", labels.isEmpty());
    }

    public void testLabelsForDocWithEmptyLabelValue() throws IOException {
        // Test that malformed labels with empty values throw IOException
        MockSortedSetDocValues labelsDocValues = new MockSortedSetDocValues(Arrays.asList("missing_value:"));

        MockTSDBDocValues tsdbDocValues = new MockTSDBDocValues(labelsDocValues);

        Labels labels = IndexUtils.labelsForDoc(0, tsdbDocValues);
        Labels expectedLabels = ByteLabels.fromStrings("missing_value", "");
        assertEquals("Labels should contain expected labels", expectedLabels, labels);
        assertEquals("", labels.get("missing_value"));
    }

    public void testLabelsForDocWithValidMultipleColons() throws IOException {
        // Test that multiple colons are handled correctly (only first colon is delimiter)
        Labels expectedLabels = ByteLabels.fromStrings(
            "validkey",
            "validvalue",
            "multiple",
            "colons:here",    // This creates key="multiple", value="colons:here"
            "normal",
            "label"             // This creates key="normal", value="label"
        );
        List<String> expectedIndexSet = expectedLabels.toIndexSet().stream().toList();
        MockSortedSetDocValues labelsDocValues = new MockSortedSetDocValues(expectedIndexSet);

        MockTSDBDocValues tsdbDocValues = new MockTSDBDocValues(labelsDocValues);

        Labels labels = IndexUtils.labelsForDoc(0, tsdbDocValues);

        assertNotNull("Labels should not be null", labels);
        assertEquals("Labels should contain expected labels", expectedLabels, labels);
    }

    public void testLabelsForDocThrowsOnMalformedLabels() throws IOException {
        // Test various malformed label formats that should throw IOException

        // Test no colon
        MockSortedSetDocValues labelsDocValues1 = new MockSortedSetDocValues(Arrays.asList("malformed_no_colon"));
        MockTSDBDocValues tsdbDocValues1 = new MockTSDBDocValues(labelsDocValues1);

        var exception1 = assertThrows(IllegalArgumentException.class, () -> { IndexUtils.labelsForDoc(0, tsdbDocValues1); });
        assertTrue(
            "Exception should mention malformed label, but got: " + exception1.getMessage(),
            exception1.getMessage().contains("Invalid key value pair: malformed_no_colon")
        );

        // Test colon at start
        MockSortedSetDocValues labelsDocValues2 = new MockSortedSetDocValues(Arrays.asList(":missing_key"));
        MockTSDBDocValues tsdbDocValues2 = new MockTSDBDocValues(labelsDocValues2);

        var exception2 = assertThrows(IllegalArgumentException.class, () -> { IndexUtils.labelsForDoc(0, tsdbDocValues2); });
        assertTrue("Exception should mention malformed label", exception2.getMessage().contains("Invalid key value pair: :missing_key"));

        // Test empty string
        MockSortedSetDocValues labelsDocValues3 = new MockSortedSetDocValues(Arrays.asList(""));
        MockTSDBDocValues tsdbDocValues3 = new MockTSDBDocValues(labelsDocValues3);

        var exception3 = assertThrows(IllegalArgumentException.class, () -> { IndexUtils.labelsForDoc(0, tsdbDocValues3); });
        assertTrue("Exception should mention malformed label", exception3.getMessage().contains("Invalid key value pair: "));
    }

    public void testLabelsForDocWithSingleLabel() throws IOException {
        Labels expectedLabels = ByteLabels.fromStrings("single", "value");
        List<String> expectedIndexSet = expectedLabels.toIndexSet().stream().toList();
        MockSortedSetDocValues labelsDocValues = new MockSortedSetDocValues(expectedIndexSet);
        MockTSDBDocValues tsdbDocValues = new MockTSDBDocValues(labelsDocValues);

        Labels labels = IndexUtils.labelsForDoc(0, tsdbDocValues);

        assertNotNull("Labels should not be null", labels);
        assertEquals("Labels should contain expected labels", expectedLabels, labels);
    }

    public void testLabelsForDocWithSpecialCharacters() throws IOException {
        Labels expectedLabels = ByteLabels.fromStrings(
            "key_with_underscores",
            "value_with_underscores",
            "key-with-dashes",
            "value-with-dashes",
            "key.with.dots",
            "value.with.dots",
            "key/with/slashes",
            "value/with/slashes"
        );
        List<String> expectedIndexSet = expectedLabels.toIndexSet().stream().toList();
        MockSortedSetDocValues labelsDocValues = new MockSortedSetDocValues(expectedIndexSet);

        MockTSDBDocValues tsdbDocValues = new MockTSDBDocValues(labelsDocValues);

        Labels labels = IndexUtils.labelsForDoc(0, tsdbDocValues);

        assertNotNull("Labels should not be null", labels);
        assertEquals("Labels should contain expected labels", expectedLabels, labels);
    }

    static class MockTSDBDocValues extends TSDBDocValues {
        private final SortedSetDocValues labelsDocValues;

        public MockTSDBDocValues(SortedSetDocValues labelsDocValues) {
            super((NumericDocValues) null, labelsDocValues);
            this.labelsDocValues = labelsDocValues;
        }

        @Override
        public SortedSetDocValues getLabelsDocValues() {
            return labelsDocValues;
        }

        @Override
        public NumericDocValues getChunkRefDocValues() {
            throw new UnsupportedOperationException("Not implemented for test");
        }

        @Override
        public BinaryDocValues getChunkDocValues() {
            throw new UnsupportedOperationException("Not implemented for test");
        }
    }

    static class MockSortedSetDocValues extends SortedSetDocValues {
        private final List<String> values;
        private int currentOrd = 0;
        private int docValueCount = 0;

        public MockSortedSetDocValues(List<String> values) {
            this.values = values;
        }

        @Override
        public long nextOrd() throws IOException {
            if (currentOrd < docValueCount) {
                return currentOrd++;
            }
            throw new IllegalStateException("Current ord exhausted");
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            if (ord >= 0 && ord < values.size()) {
                return new BytesRef(values.get((int) ord));
            }
            return null;
        }

        @Override
        public long getValueCount() {
            return values.size();
        }

        @Override
        public int docValueCount() {
            return docValueCount;
        }

        @Override
        public int docID() {
            return 0;
        }

        @Override
        public int nextDoc() throws IOException {
            return NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
            return NO_MORE_DOCS;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            if (target == 0) {
                currentOrd = 0;
                docValueCount = values.size();
                return true;
            }
            return false;
        }

        @Override
        public long cost() {
            return values.size();
        }
    }
}
