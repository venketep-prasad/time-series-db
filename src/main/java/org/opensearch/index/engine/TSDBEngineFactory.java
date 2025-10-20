/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import java.io.IOException;

/**
 * Factory for creating TSDBEngine instances.
 * <p>
 * This factory provides the OpenSearch engine implementation specialized for time series data
 * processing and storage. It integrates the time series functionality with OpenSearch's
 * engine architecture.
 */
public class TSDBEngineFactory implements EngineFactory {

    /**
     * Default constructor for TSDBEngineFactory.
     */
    public TSDBEngineFactory() {

    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        try {
            return new TSDBEngine(config);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create TSDBEngine", e);
        }
    }
}
