/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

public class FallbackSeriesBinaryPlanNodeTests extends BinaryPlanNodeTests {

    protected BinaryPlanNode getBinaryPlanNode() {
        return new FallbackSeriesBinaryPlanNode(1);
    }

    public void testFallbackSeriesBinaryPlanNode() {
        verifyPlanNodeName("FALLBACK_SERIES");
        verifyVisitorAccept();
    }
}
