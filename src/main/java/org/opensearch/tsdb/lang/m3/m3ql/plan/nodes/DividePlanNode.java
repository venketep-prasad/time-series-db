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

public class DividePlanNode extends BinaryByTagsPlanNode {
    public DividePlanNode(int id, List<String> tags) {
        super(id, tags);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "DIVIDE_SERIES(groupBy=%s)", this.getTags());
    }
}
