/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class DiffPlanNode extends BinaryByTagsPlanNode {
    private final boolean keepNans;

    public DiffPlanNode(int id, boolean keepNans, List<String> tags) {
        super(id, tags);
        this.keepNans = keepNans;
    }

    public DiffPlanNode(int id) {
        this(id, false, Collections.emptyList());
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "DIFF(keepNans=%s,groupBy=%s)", keepNans, this.getTags());
    }

    public boolean isKeepNans() {
        return keepNans;
    }
}
