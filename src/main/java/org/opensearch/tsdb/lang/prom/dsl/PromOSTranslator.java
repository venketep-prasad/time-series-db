/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.dsl;

import org.opensearch.ExceptionsHelper;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tsdb.core.utils.Constants;
import org.opensearch.tsdb.lang.prom.promql.parser.generated.PromQLParser;
import org.opensearch.tsdb.lang.prom.promql.parser.generated.ParseException;
import org.opensearch.tsdb.lang.prom.promql.parser.nodes.RootNode;
import org.opensearch.tsdb.lang.prom.promql.plan.PromASTConverter;
import org.opensearch.tsdb.lang.prom.promql.plan.nodes.PromPlanNode;

import java.util.concurrent.TimeUnit;

/**
 * PromOSTranslator is responsible for translating PromQL to OpenSearch DSL.
 *
 * <p>Translation flow:
 * <ol>
 *   <li>Parse PromQL query string → AST (Abstract Syntax Tree)</li>
 *   <li>Convert AST → Logical Plan</li>
 *   <li>(Optional) Optimize Plan</li>
 *   <li>Translate Plan → OpenSearch SearchSourceBuilder</li>
 * </ol>
 */
public class PromOSTranslator {

    private PromOSTranslator() {}

    /**
     * Translates a PromQL query string to OpenSearch SearchSourceBuilder.
     *
     * @param query The PromQL query string
     * @param params Query parameters (time range, step, etc.)
     * @return SearchSourceBuilder ready for execution
     * @throws RuntimeException if parsing or translation fails
     */
    public static SearchSourceBuilder translate(String query, Params params) {
        try {
            return translateInternal(query, params);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        } catch (Throwable t) {
            // Catch any Error thrown by the JavaCC generated parser
            throw new RuntimeException("Failed to parse PromQL query", t);
        }
    }

    private static SearchSourceBuilder translateInternal(String query, Params params) throws ParseException {
        // 1. Parse query and create AST
        RootNode astRoot = PromQLParser.parse(query);

        // 2. Convert AST to logical plan
        PromASTConverter astConverter = new PromASTConverter();
        PromPlanNode planRoot = astConverter.buildPlan(astRoot);

        // 3. (TODO) Apply optimizations to the plan

        // 4. Convert to SearchSourceBuilder
        PromSourceBuilderVisitor visitor = new PromSourceBuilderVisitor(params);
        PromSourceBuilderVisitor.ComponentHolder holder = visitor.process(planRoot);

        return holder.toSearchSourceBuilder().profile(params.profile()).trackTotalHits(false);  // Don't need total hits for TSDB queries
    }

    /**
     * Query parameters used during PromQL translation.
     *
     * @param timeUnit Time unit for startTime and endTime
     * @param startTime Query start time in the specified time unit
     * @param endTime Query end time in the specified time unit
     * @param step Step interval for aggregations in the specified time unit
     * @param lookbackDelta Override lookback period (in milliseconds). Use 0 for default behavior.
     * @param pushdown Enable pushdown optimizations (shard-level execution)
     * @param profile Enable profiling for debugging
     */
    public record Params(TimeUnit timeUnit, long startTime, long endTime, long step, long lookbackDelta, boolean pushdown,
        boolean profile) {

        /**
         * Validation for params.
         */
        public Params {
            if (startTime >= endTime) {
                throw new IllegalArgumentException("Start time must be less than end time");
            }
            if (step <= 0) {
                throw new IllegalArgumentException("Step must be positive");
            }
            if (lookbackDelta < 0) {
                throw new IllegalArgumentException("Lookback delta must be non-negative");
            }
        }

        /**
         * Constructor with default time unit (milliseconds).
         */
        public Params(long startTime, long endTime, long step, long lookbackDelta, boolean pushdown, boolean profile) {
            this(Constants.Time.DEFAULT_TIME_UNIT, startTime, endTime, step, lookbackDelta, pushdown, profile);
        }
    }
}
