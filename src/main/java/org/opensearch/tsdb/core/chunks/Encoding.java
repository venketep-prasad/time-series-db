/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.chunks;

/**
 * Chunk encoding
 */
public enum Encoding {
    /** XOR encoding - uses XOR-based compression for efficient storage */
    XOR;
}
