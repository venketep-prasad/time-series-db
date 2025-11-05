/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import java.util.List;
import java.util.Locale;

public class AsPercentPlanNode extends BinaryByTagsPlanNode {
    /**
     * Constructor for BinaryPlanNode.
     *
     * @param id   node id
     * @param tags
     */
    public AsPercentPlanNode(int id, List<String> tags) {
        super(id, tags);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "AS_PERCENT(groupBy=%s)", this.getTags());
    }
}
