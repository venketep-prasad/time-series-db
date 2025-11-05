/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import java.util.List;

public abstract class BinaryByTagsPlanNode extends BinaryPlanNode {
    private final List<String> tags;

    public BinaryByTagsPlanNode(int id, List<String> tags) {
        super(id);
        this.tags = tags;
    }

    /**
     * Returns a list of aggregation keys
     * @return List of Strings representing the aggregation key tags, null indicates aggregate all series without grouping.
     */
    public List<String> getTags() {
        return tags;
    }
}
