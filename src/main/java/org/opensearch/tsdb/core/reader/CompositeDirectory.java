/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A composite directory that provides a unified view over multiple underlying directories.
 * Note : Technically Directory is not supposed to contain subdirectories. But this class is needed because
 * {@link TSDBDirectoryReader} needs to call {@link org.apache.lucene.index.DirectoryReader}'s constructor which requires a {@link Directory}.
 *
 *
 * CompositeDirectory allows querying across multiple Lucene directories as if they were
 * a single directory, enabling unified queries over both live series and closed chunk indices.
 *
 * Most operations that takes a single file name are unsupported because there might be duplicate file names across the underlying directories.
 */
public class CompositeDirectory extends Directory {
    private final List<Directory> directories;

    /**
     * Creates a new CompositeDirectory that combines multiple underlying directories.
     * @param directories the list of directories to combine
     */
    public CompositeDirectory(List<Directory> directories) {
        this.directories = directories;
    }

    @Override
    public String[] listAll() throws IOException {
        Set<String> allFiles = new LinkedHashSet<>();
        for (Directory directory : directories) {
            String[] files = directory.listAll();
            Collections.addAll(allFiles, files);
        }
        return allFiles.toArray(new String[0]);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        throw new UnsupportedOperationException(
            "Delete by file name is not supported in CompositeDirectory as there might be duplicate in file names"
        );
    }

    @Override
    public long fileLength(String name) throws IOException {
        throw new UnsupportedOperationException(
            "File length by name is not supported in CompositeDirectory as there might be duplicate in file names"
        );
    }

    @Override
    public IndexOutput createOutput(String s, IOContext ioContext) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexOutput createTempOutput(String s, String s1, IOContext ioContext) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync(Collection<String> collection) throws IOException {
        for (Directory directory : directories) {
            directory.sync(collection);
        }
    }

    @Override
    public void syncMetaData() throws IOException {
        for (Directory directory : directories) {
            directory.syncMetaData();
        }
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        throw new UnsupportedOperationException(
            "Rename by file name is not supported in CompositeDirectory as there might be duplicate in file names"
        );
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        throw new UnsupportedOperationException(
            "Open input by file name is not supported in CompositeDirectory as there might be duplicate in file names"
        );
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        List<Lock> locks = new ArrayList<>();
        try {
            for (Directory directory : directories) {
                locks.add(directory.obtainLock(name));
            }
            return new CompositeLock(locks);
        } catch (IOException e) {
            // If we fail to obtain any lock, release all previously obtained locks
            for (Lock lock : locks) {
                try {
                    lock.close();
                } catch (IOException closeEx) {
                    e.addSuppressed(closeEx);
                }
            }
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        IOException firstException = null;
        for (Directory d : directories) {
            try {
                d.close();
            } catch (IOException e) {
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    @Override
    public String toString() {
        return "CompositeDirectory(" + directories + ")";
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        Set<String> allPendingDeletions = new java.util.LinkedHashSet<>();
        for (Directory directory : directories) {
            allPendingDeletions.addAll(directory.getPendingDeletions());
        }
        return allPendingDeletions;
    }

    private static class CompositeLock extends Lock {
        private final List<Lock> locks;

        CompositeLock(List<Lock> locks) {
            this.locks = locks;
        }

        @Override
        public void close() throws IOException {
            IOException firstException = null;
            for (Lock lock : locks) {
                try {
                    lock.close();
                } catch (IOException e) {
                    if (firstException == null) {
                        firstException = e;
                    } else {
                        firstException.addSuppressed(e);
                    }
                }
            }
            if (firstException != null) {
                throw firstException;
            }
        }

        @Override
        public void ensureValid() throws IOException {
            for (Lock lock : locks) {
                lock.ensureValid();
            }
        }
    }
}
