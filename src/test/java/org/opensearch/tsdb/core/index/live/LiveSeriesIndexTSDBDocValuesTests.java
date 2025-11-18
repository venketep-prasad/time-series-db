/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class LiveSeriesIndexTSDBDocValuesTests extends OpenSearchTestCase {

    public void testConstructorAndBasicMethods() throws IOException {
        MockNumericDocValues chunkRefDocValues = new MockNumericDocValues();
        MockSortedSetDocValues labelsDocValues = new MockSortedSetDocValues();

        LiveSeriesIndexTSDBDocValues tsdbDocValues = new LiveSeriesIndexTSDBDocValues(chunkRefDocValues, labelsDocValues);

        assertSame("Should return the same chunk ref doc values", chunkRefDocValues, tsdbDocValues.getChunkRefDocValues());
        assertSame("Should return the same labels doc values", labelsDocValues, tsdbDocValues.getLabelsDocValues());
    }

    public void testGetChunkDocValuesThrowsException() throws IOException {
        MockNumericDocValues chunkRefDocValues = new MockNumericDocValues();
        MockSortedSetDocValues labelsDocValues = new MockSortedSetDocValues();

        LiveSeriesIndexTSDBDocValues tsdbDocValues = new LiveSeriesIndexTSDBDocValues(chunkRefDocValues, labelsDocValues);

        UnsupportedOperationException exception = expectThrows(UnsupportedOperationException.class, tsdbDocValues::getChunkDocValues);
        assertEquals("Live Series Index does not support chunk doc values", exception.getMessage());
    }

    public void testWithNullValues() throws IOException {
        LiveSeriesIndexTSDBDocValues tsdbDocValues = new LiveSeriesIndexTSDBDocValues(null, null);

        assertNull("Should return null chunk ref doc values", tsdbDocValues.getChunkRefDocValues());
        assertNull("Should return null labels doc values", tsdbDocValues.getLabelsDocValues());

        expectThrows(UnsupportedOperationException.class, tsdbDocValues::getChunkDocValues);
    }

    public void testWithRealValues() throws IOException {
        MockNumericDocValues chunkRefDocValues = new MockNumericDocValues();
        chunkRefDocValues.setValue(123L);

        MockSortedSetDocValues labelsDocValues = new MockSortedSetDocValues();

        LiveSeriesIndexTSDBDocValues tsdbDocValues = new LiveSeriesIndexTSDBDocValues(chunkRefDocValues, labelsDocValues);

        // Test that we can actually use the doc values
        assertTrue("Should advance to document", chunkRefDocValues.advanceExact(0));
        assertEquals("Should return correct chunk reference", 123L, chunkRefDocValues.longValue());

        assertTrue("Should advance labels doc values", labelsDocValues.advanceExact(0));
    }

    static class MockNumericDocValues extends NumericDocValues {
        private long value = 0L;

        public void setValue(long value) {
            this.value = value;
        }

        @Override
        public long longValue() throws IOException {
            return value;
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
            return target == 0;
        }

        @Override
        public long cost() {
            return 1;
        }
    }

    static class MockSortedSetDocValues extends SortedSetDocValues {
        @Override
        public long nextOrd() throws IOException {
            return -1; // NO_MORE_ORDS
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            return new BytesRef("test");
        }

        @Override
        public long getValueCount() {
            return 1;
        }

        @Override
        public int docValueCount() {
            return 1;
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
            return target == 0;
        }

        @Override
        public long cost() {
            return 1;
        }
    }
}
