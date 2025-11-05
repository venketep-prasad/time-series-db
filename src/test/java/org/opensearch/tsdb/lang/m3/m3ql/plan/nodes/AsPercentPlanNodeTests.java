/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import java.util.Collections;

public class AsPercentPlanNodeTests extends BinaryPlanNodeTests {

    protected BinaryPlanNode getBinaryPlanNode() {
        return new AsPercentPlanNode(1, Collections.emptyList());
    }

    public void testAsPercentPlanNode() {
        verifyPlanNodeName("AS_PERCENT(groupBy=[])");
        verifyVisitorAccept();
    }
}
