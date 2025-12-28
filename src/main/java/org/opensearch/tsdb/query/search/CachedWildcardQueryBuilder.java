/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.metrics.TSDBMetrics;

import java.io.IOException;

/**
 * A WildcardQueryBuilder that caches the resulting Lucene Query objects using
 * OpenSearch's built-in Cache implementation to avoid redundant automaton construction.
 * <p>
 * This query builder extends {@link WildcardQueryBuilder} and only overrides {@link #doToQuery}
 * to add caching. All other behavior (serialization, parsing, rewriting) is inherited from the
 * parent class.
 * <p>
 * The cache is shared globally across all indices and stores Query objects keyed by:
 * <ul>
 *   <li>Field name</li>
 *   <li>Field type class (different field types produce different queries)</li>
 *   <li>Wildcard pattern</li>
 *   <li>Rewrite method (affects query construction)</li>
 *   <li>Case sensitivity setting</li>
 * </ul>
 * <p>
 * Cache configuration can be customized via dynamic cluster settings:
 * <ul>
 *   <li>tsdb_engine.search.wildcard_query.cache.max_size (default: 0, disabled)</li>
 *   <li>tsdb_engine.search.wildcard_query.cache.expire_after (default: 1 hour)</li>
 * </ul>
 * <p>
 * Note: Caching is disabled by default. To enable, set max_size to a positive value.
 * <p>
 * Usage:
 * <pre>{@code
 * // Instead of:
 * QueryBuilders.wildcardQuery("labels", "name:server-*")
 *
 * // Use:
 * new CachedWildcardQueryBuilder("labels", "name:server-*")
 * }</pre>
 * <p>
 * Benefits:
 * <ul>
 *   <li>Avoids rebuilding wildcard automaton for repeated patterns</li>
 *   <li>Respects field mapper configuration (analyzers, normalizers, etc.)</li>
 *   <li>Works with any field type that supports wildcard queries</li>
 *   <li>Thread-safe with minimal overhead</li>
 *   <li>Configurable cache size and expiration via dynamic index settings</li>
 * </ul>
 * <p>
 * Performance impact:
 * <ul>
 *   <li>Cache hit: Skips expensive automaton construction entirely</li>
 *   <li>Cache miss: Same cost as standard wildcard query</li>
 *   <li>Memory: ~1-10KB per cached pattern (depending on complexity)</li>
 * </ul>
 */
public class CachedWildcardQueryBuilder extends WildcardQueryBuilder {

    private static final Logger logger = LogManager.getLogger(CachedWildcardQueryBuilder.class);

    public static final String NAME = "cached_wildcard";

    // Shared cache across all instances and all indices
    // Uses OpenSearch's built-in LRU cache with time-based eviction
    // Default initialization - will be overwritten by initializeCache()
    private static volatile Cache<CacheKey, Query> QUERY_CACHE = createCache(0, TimeValue.timeValueHours(1));

    /**
     * Creates a new CachedWildcardQueryBuilder.
     *
     * @param fieldName the field to query
     * @param value the wildcard pattern (* and ? supported)
     */
    public CachedWildcardQueryBuilder(String fieldName, String value) {
        super(fieldName, value);
    }

    /**
     * Read from stream (for deserialization across nodes).
     *
     * @param in the stream to read from
     * @throws IOException if reading from the stream fails
     */
    public CachedWildcardQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Parse from XContent (for REST API requests).
     * Delegates to WildcardQueryBuilder's fromXContent for parsing the nested wildcard query.
     *
     * @param parser the XContent parser
     * @return a new CachedWildcardQueryBuilder
     * @throws IOException if parsing fails
     */
    public static CachedWildcardQueryBuilder fromXContent(XContentParser parser) throws IOException {
        // When called by the framework, parser is positioned at START_OBJECT after "cached_wildcard"
        // Expected JSON: {"cached_wildcard": {"wildcard": {"field": {"wildcard": "pattern*"}}}}

        // Move to FIELD_NAME - expect "wildcard"
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new IllegalArgumentException("[cached_wildcard] query malformed, expected 'wildcard' field");
        }

        String fieldName = parser.currentName();
        if (!"wildcard".equals(fieldName)) {
            throw new IllegalArgumentException("[cached_wildcard] query expects 'wildcard' field, found [" + fieldName + "]");
        }

        // Move to START_OBJECT for the wildcard content
        parser.nextToken();

        // Delegate to parent's fromXContent to parse the wildcard query
        WildcardQueryBuilder wildcardBuilder = WildcardQueryBuilder.fromXContent(parser);

        // Consume the END_OBJECT token that closes the "wildcard" field
        parser.nextToken();

        // Create CachedWildcardQueryBuilder with the same parameters
        CachedWildcardQueryBuilder builder = new CachedWildcardQueryBuilder(wildcardBuilder.fieldName(), wildcardBuilder.value());

        // Copy all optional fields from the parsed wildcard builder
        if (wildcardBuilder.caseInsensitive() != false) {
            builder.caseInsensitive(wildcardBuilder.caseInsensitive());
        }
        if (wildcardBuilder.rewrite() != null) {
            builder.rewrite(wildcardBuilder.rewrite());
        }
        if (wildcardBuilder.boost() != DEFAULT_BOOST) {
            builder.boost(wildcardBuilder.boost());
        }
        if (wildcardBuilder.queryName() != null) {
            builder.queryName(wildcardBuilder.queryName());
        }

        return builder;
    }

    /**
     * Serialize to XContent using "cached_wildcard" as the query name.
     * Wraps the parent's wildcard query structure.
     */
    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);  // "cached_wildcard"
        super.doXContent(builder, params);  // Writes the complete "wildcard" structure
        builder.endObject();
    }

    /**
     * Creates a cache instance with the specified settings.
     *
     * @param maxSize the maximum number of entries
     * @param expireAfterAccess the time after which unused entries are evicted
     * @return a new cache instance
     */
    private static Cache<CacheKey, Query> createCache(int maxSize, TimeValue expireAfterAccess) {
        if (maxSize <= 0) {
            // Return a no-op cache if caching is disabled
            return CacheBuilder.<CacheKey, Query>builder().setMaximumWeight(0).build();
        }
        return CacheBuilder.<CacheKey, Query>builder().setMaximumWeight(maxSize).setExpireAfterAccess(expireAfterAccess).build();
    }

    /**
     * Initialize cache with cluster-level settings and register dynamic update listeners.
     * Called from TSDBPlugin.createComponents() once per node startup.
     *
     * @param clusterSettings the cluster settings for registering dynamic listeners
     * @param settings the current cluster settings
     */
    public static void initializeCache(ClusterSettings clusterSettings, Settings settings) {
        // Initialize cache with current settings
        QUERY_CACHE = createCache(
            TSDBPlugin.TSDB_ENGINE_WILDCARD_QUERY_CACHE_MAX_SIZE.get(settings),
            TSDBPlugin.TSDB_ENGINE_WILDCARD_QUERY_CACHE_EXPIRE_AFTER.get(settings)
        );

        // Register listener for both settings together using BiConsumer
        clusterSettings.addSettingsUpdateConsumer(
            TSDBPlugin.TSDB_ENGINE_WILDCARD_QUERY_CACHE_MAX_SIZE,
            TSDBPlugin.TSDB_ENGINE_WILDCARD_QUERY_CACHE_EXPIRE_AFTER,
            (newMaxSize, newExpiration) -> {
                // Recreate cache with both new values
                QUERY_CACHE = createCache(newMaxSize, newExpiration);
                logger.info("Wildcard query cache recreated with maxSize={}, expiration={}", newMaxSize, newExpiration);
            }
        );
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        // Get the field mapper
        MappedFieldType fieldType = context.fieldMapper(fieldName());

        if (fieldType == null) {
            throw new IllegalArgumentException("Field [" + fieldName() + "] does not exist in mapping");
        }

        // Create cache key including ALL parameters that affect query construction
        CacheKey cacheKey = new CacheKey(
            fieldType.name(),           // Actual field name
            value(),                    // Wildcard pattern
            rewrite(),                  // Rewrite method
            caseInsensitive()           // Case sensitivity
        );

        // Get from cache or create via field mapper
        try {
            Query result = QUERY_CACHE.computeIfAbsent(cacheKey, key -> {
                // This calls KeywordFieldMapper.wildcardQuery()
                // Automaton is built here on cache miss
                return super.doToQuery(context);
            });
            emitCacheStats();
            return result;
        } catch (Exception e) {
            // Unwrap ExecutionException if present
            if (e.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw new RuntimeException("Failed to create cached wildcard query for field [" + fieldName() + "]", e);
        }
    }

    private void emitCacheStats() {
        // Capture cache stats after the operation
        Cache.CacheStats statsAfter = QUERY_CACHE.stats();
        if (statsAfter.getHits() > 0) {
            TSDBMetrics.incrementCounter(TSDBMetrics.SEARCH.wildcardCacheHits, statsAfter.getHits());
        }
        if (statsAfter.getMisses() > 0) {
            TSDBMetrics.incrementCounter(TSDBMetrics.SEARCH.wildcardCacheMisses, statsAfter.getMisses());
        }
        if (statsAfter.getEvictions() > 0) {
            TSDBMetrics.incrementCounter(TSDBMetrics.SEARCH.wildcardCacheEvictions, statsAfter.getEvictions());
        }
        // Record current cache size
        TSDBMetrics.recordHistogram(TSDBMetrics.SEARCH.wildcardCacheSize, QUERY_CACHE.count());

    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Get cache statistics for monitoring.
     *
     * @return cache stats including hit count, miss count, eviction count
     */
    public static Cache.CacheStats getCacheStats() {
        return QUERY_CACHE.stats();
    }

    /**
     * Get the number of cache hits.
     *
     * @return total number of successful cache lookups
     */
    public static long getCacheHits() {
        return QUERY_CACHE.stats().getHits();
    }

    /**
     * Get the number of cache misses.
     *
     * @return total number of cache lookups that required query construction
     */
    public static long getCacheMisses() {
        return QUERY_CACHE.stats().getMisses();
    }

    /**
     * Get the number of cache evictions.
     *
     * @return total number of entries evicted from the cache
     */
    public static long getCacheEvictions() {
        return QUERY_CACHE.stats().getEvictions();
    }

    /**
     * Get current cache size (number of entries).
     *
     * @return the number of entries currently in the cache
     */
    public static long getCacheSize() {
        return QUERY_CACHE.count();
    }

    /**
     * Clear the cache and reset statistics (useful for testing).
     * Recreates the cache to ensure both entries and stats are reset.
     */
    public static synchronized void clearCache() {
        QUERY_CACHE = createCache(0, TimeValue.timeValueHours(1));
    }

    /**
     * Cache key including all parameters that affect query construction.
     */
    private record CacheKey(String fieldName, String pattern, String rewriteMethod, boolean caseInsensitive) {
    }
}
