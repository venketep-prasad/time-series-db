/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.index.live.MemChunkReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reference manager for TSDBDirectoryReader that handles initiation and refreshing of TSDBDirectoryReader instances.
 * It manages acquiring and releasing of underlying DirectoryReaders from live series index and closed chunk indexes.
 * Life Cycle:
 * TSDBDirectoryReaderReferenceManager is created once per TSDBEngine instance and lives as long as the TSDBEngine.
 * **/
@SuppressForbidden(reason = "Reference managing is required here")
public class TSDBDirectoryReaderReferenceManager extends ReferenceManager<OpenSearchDirectoryReader> {

    private static final Logger log = LogManager.getLogger(TSDBDirectoryReaderReferenceManager.class);
    private final ReaderManager liveSeriesIndexReaderManager;
    private final ClosedChunkIndexManager closedChunkIndexManager;
    private final MemChunkReader memChunkReader;
    private final ShardId shardId;

    private volatile List<ReaderManager> closedChunkIndexReaderManagers;

    /**
     * Creates a new TSDBDirectoryReaderReferenceManager.
     * @param liveSeriesIndexReaderManager the reader manager for live series index
     * @param closedChunkIndexManager the manager for closed chunk indices
     * @param memChunkReader the reader for memory chunks
     * @param shardId the shard identifier
     * @throws IOException if an I/O error occurs during initialization
     */
    // TODO : Pass in data structure to hold already mmaped chunks
    public TSDBDirectoryReaderReferenceManager(
        ReaderManager liveSeriesIndexReaderManager,
        ClosedChunkIndexManager closedChunkIndexManager,
        MemChunkReader memChunkReader,
        ShardId shardId
    ) throws IOException {

        this.liveSeriesIndexReaderManager = liveSeriesIndexReaderManager;
        this.closedChunkIndexManager = closedChunkIndexManager;
        this.closedChunkIndexReaderManagers = closedChunkIndexManager.getReaderManagers();
        this.memChunkReader = memChunkReader;
        this.shardId = shardId;

        // initiate the MDR here
        this.current = OpenSearchDirectoryReader.wrap(
            creatNewTSDBDirectoryReader(liveSeriesIndexReaderManager, closedChunkIndexManager, memChunkReader, 0L),
            shardId
        );
    }

    private TSDBDirectoryReader creatNewTSDBDirectoryReader(
        ReaderManager liveSeriesIndexReaderManager,
        ClosedChunkIndexManager closedChunkIndexManager,
        MemChunkReader memchunkReader,
        long currentVersion
    ) throws IOException {
        // Collect all closed chunk index reader managers
        List<ReaderManager> allClosedReaderManagers = closedChunkIndexManager.getReaderManagers();

        // Acquire DirectoryReader instances from all ReaderManagers
        DirectoryReader liveReader = null;
        List<DirectoryReader> closedReaders = new ArrayList<>();

        try {
            liveSeriesIndexReaderManager.maybeRefreshBlocking();
            liveReader = liveSeriesIndexReaderManager.acquire();

            for (ReaderManager readerManager : allClosedReaderManagers) {
                readerManager.maybeRefreshBlocking();
                closedReaders.add(readerManager.acquire());
            }

            log.info("Refreshing closed reader managers, total readers: {}", closedReaders.size());
            return new TSDBDirectoryReader(liveReader, closedReaders, memchunkReader, currentVersion + 1);

        } catch (IOException | AlreadyClosedException e) {
            log.error("Error creating TSDBDirectoryReader: ", e);
            throw e;
        } finally {
            if (liveReader != null) {
                liveSeriesIndexReaderManager.release(liveReader);
            }
            for (int i = 0; i < closedReaders.size(); i++) {
                if (i < allClosedReaderManagers.size()) {
                    allClosedReaderManagers.get(i).release(closedReaders.get(i));
                }
            }
        }
    }

    @Override
    protected void decRef(OpenSearchDirectoryReader reference) throws IOException {
        // Only calling decRef on OpenSearchDirectoryReader which will call decRef on underlying TSDBDirectoryReader
        // This does not decRef the underlying DirectoryReaders as they are managed by their respective ReaderManagers
        reference.decRef();
    }

    /**
     * Refreshes the reader if needed. Performs either a structural refresh (if indexes were added/removed)
     * or a lightweight refresh (if only data within existing indexes changed).
     *
     * @param referenceToRefresh the current reader reference
     * @return a new reader if refresh occurred, or null if no refresh was needed
     * @throws IOException if an I/O error occurs during refresh
     */
    @Override
    protected OpenSearchDirectoryReader refreshIfNeeded(OpenSearchDirectoryReader referenceToRefresh) throws IOException {
        List<ReaderManager> currentReaderManagers = closedChunkIndexManager.getReaderManagers();

        if (!this.closedChunkIndexReaderManagers.equals(currentReaderManagers)) {
            // Structural change detected - indexes were added or removed
            final OpenSearchDirectoryReader reader = OpenSearchDirectoryReader.wrap(
                creatNewTSDBDirectoryReader(
                    liveSeriesIndexReaderManager,
                    closedChunkIndexManager,
                    memChunkReader,
                    this.current.getVersion()
                ),
                shardId
            );

            // Update snapshot to prevent redundant structural refreshes
            this.closedChunkIndexReaderManagers = currentReaderManagers;

            log.info("Refreshed the tsdb directory reader");
            return reader;

        } else {
            log.info("No changes detected for refreshing the tsdb directory reader");
            // No structural change - attempt lightweight refresh
            return (OpenSearchDirectoryReader) DirectoryReader.openIfChanged(referenceToRefresh);
        }
    }

    @Override
    protected boolean tryIncRef(OpenSearchDirectoryReader reference) {
        return reference.tryIncRef();
    }

    @Override
    protected int getRefCount(OpenSearchDirectoryReader reference) {
        return reference.getRefCount();
    }
}
