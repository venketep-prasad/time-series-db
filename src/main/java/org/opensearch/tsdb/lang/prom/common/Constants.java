/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.prom.common;

/**
 * Constants for PromQL.
 */
public class Constants {

    /**
     * PromQL function names.
     */
    public static class Functions {
        // Aggregation operators
        public static final String SUM = "sum";
        public static final String AVG = "avg";
        public static final String MIN = "min";
        public static final String MAX = "max";
        public static final String COUNT = "count";

        // Rate/Increase functions
        public static final String RATE = "rate";
        public static final String IRATE = "irate";
        public static final String INCREASE = "increase";

        // Math functions
        public static final String ABS = "abs";
        public static final String CEIL = "ceil";
        public static final String FLOOR = "floor";
        public static final String ROUND = "round";

        private Functions() {}
    }

    private Constants() {}
}
