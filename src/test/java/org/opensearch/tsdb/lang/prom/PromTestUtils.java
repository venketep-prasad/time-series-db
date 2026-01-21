/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom;

import org.opensearch.tsdb.lang.prom.promql.parser.nodes.PromASTNode;
import org.opensearch.tsdb.lang.prom.promql.plan.nodes.PromPlanNode;

import java.io.PrintStream;

/**
 * Utility functions for PromQL tests.
 */
public class PromTestUtils {

    /**
     * Prints the AST node and its children recursively.
     *
     * @param node  The AST node to print.
     * @param depth The current depth in the tree, used for indentation.
     * @param ps    The PrintStream to which the output will be written.
     */
    public static void printAST(PromASTNode node, int depth, PrintStream ps) {
        for (int i = 0; i < depth; i++) {
            ps.print("  ");
        }
        ps.println(node.getClass().getSimpleName());

        for (PromASTNode childNode : node.getChildren()) {
            printAST(childNode, depth + 1, ps);
        }
    }

    /**
     * Prints the plan node and its children recursively.
     *
     * @param node  The plan node to print.
     * @param depth The current depth in the tree, used for indentation.
     * @param ps    The PrintStream to which the output will be written.
     */
    public static void printPlan(PromPlanNode node, int depth, PrintStream ps) {
        for (int i = 0; i < depth; i++) {
            ps.print("  ");
        }
        ps.println(node.getExplainName());

        for (PromPlanNode childNode : node.getChildren()) {
            printPlan(childNode, depth + 1, ps);
        }
    }
}
