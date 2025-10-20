/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.opensearch.test.OpenSearchTestCase;

public class TSDBEngineFactoryTests extends OpenSearchTestCase {

    public void testFactoryInstantiation() {
        TSDBEngineFactory factory = new TSDBEngineFactory();
        assertNotNull(factory);
        assertTrue(factory instanceof EngineFactory);
    }
}
