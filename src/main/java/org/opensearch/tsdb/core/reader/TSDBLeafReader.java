/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.lucene.index.LeafReader;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;
import java.util.List;

/**
 * Abstract base class for reading tsdb data from Lucene leaf readers.
 * Extends SequentialStoredFieldsLeafReader to provide specialized functionality
 * for accessing time series chunks and labels associated with documents.
 */
public abstract class TSDBLeafReader extends SequentialStoredFieldsLeafReader {

    /**
     * <p>Construct a StoredFieldsFilterLeafReader based on the specified base reader.
     * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
     *
     *  @param in :  specified base reader.
     */
    public TSDBLeafReader(LeafReader in) {
        super(in);
    }

    /**
     * Retrieve the TSDBDocValues instance containing various DocValues types.
     * This method must be implemented by subclasses to provide access to the
     * appropriate DocValues for chunks and labels.
     *
     * @return a TSDBDocValues object encapsulating the relevant DocValues
     * @throws IOException if an error occurs while accessing the index
     */
    public abstract TSDBDocValues getTSDBDocValues() throws IOException;

    /**
     * Retrieve the list of chunks associated with a given document ID.
     * Each document may reference one or more chunks, which are returned as a list.
     *
     * @param docId the document ID to retrieve chunks for
     * @param tsdbDocValues the TSDBDocValues containing doc values for chunks. tsdbDocValues should be acquired in the same thread that calls this method.
     * @return a list of Chunk objects associated with the document
     * @throws IOException if an error occurs while accessing the index
     */
    public abstract List<ChunkIterator> chunksForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException;

    /**
     * Parse labels from SortedSetDocValues into a ByteLabels object.
     * Labels are stored as "key:value" strings in the SortedSetDocValues.
     * @param docId the document ID to retrieve labels for
     * @param tsdbDocValues the TSDBDocValues containing doc values for labels. tsdbDocValues should be acquired in the same thread that calls this method.
     * @return a Labels object representing the labels associated with the document
     * @throws IOException if an error occurs while accessing the index
     */
    public abstract Labels labelsForDoc(int docId, TSDBDocValues tsdbDocValues) throws IOException;

}
