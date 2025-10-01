/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import org.apache.lucene.store.Directory;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;

/**
 * Unit tests for the TSDBStore class.
 */
public class TSDBStoreTests extends OpenSearchTestCase {

    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build()
    );

    /**
     * Test that TSDBStore can be instantiated.
     */
    public void testTSDBStoreInstantiation() throws Exception {
        final ShardId shardId = new ShardId("test-index", "_na_", 1);
        Directory directory = newDirectory();
        final Path path = createTempDir().resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        ShardPath shardPath = new ShardPath(false, path, path, shardId);

        // Test instantiation
        TSDBStore tsdbStore = new TSDBStore(
            shardId,
            INDEX_SETTINGS,
            directory,
            new DummyShardLock(shardId),
            Store.OnClose.EMPTY,
            shardPath
        );

        // Verify the store was created successfully
        assertNotNull(tsdbStore);
        assertEquals(shardId, tsdbStore.shardId());
        assertEquals(shardPath, tsdbStore.shardPath());

        // Clean up properly
        tsdbStore.close();
        directory.close();
    }
}
