/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import java.util.Collections;

public class DiffPlanNodeTests extends BinaryPlanNodeTests {

    protected BinaryPlanNode getBinaryPlanNode() {
        return new DiffPlanNode(1);
    }

    public void testDiffPlanNode() {
        verifyPlanNodeName("DIFF(keepNans=false,groupBy=[])");
        verifyVisitorAccept();
    }

    public void testKeepNans() {
        assertFalse(new DiffPlanNode(1, false, Collections.emptyList()).isKeepNans());
        assertTrue(new DiffPlanNode(1, true, Collections.emptyList()).isKeepNans());
    }
}
