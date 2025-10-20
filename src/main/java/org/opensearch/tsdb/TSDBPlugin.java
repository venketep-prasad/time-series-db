/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import org.opensearch.common.settings.Setting;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.TSDBEngineFactory;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;

import java.util.List;
import java.util.Optional;

/**
 * Plugin for time-series database (TSDB) engine
 */
public class TSDBPlugin extends Plugin implements SearchPlugin, EnginePlugin {

    /**
     * This setting identifies if the tsdb engine is enabled for the index.
     */
    public static final Setting<Boolean> TSDB_ENGINE_ENABLED = Setting.boolSetting(
        "index.tsdb_engine.enabled",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Default constructor
     */
    public TSDBPlugin() {}

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(TSDB_ENGINE_ENABLED);
    }

    @Override
    public List<AggregationSpec> getAggregations() {
        return List.of(
            // Register TimeSeriesUnfoldAggregation
            new AggregationSpec(
                TimeSeriesUnfoldAggregationBuilder.NAME,
                TimeSeriesUnfoldAggregationBuilder::new,
                TimeSeriesUnfoldAggregationBuilder::parse
            ).addResultReader(InternalTimeSeries::new).setAggregatorRegistrar(TimeSeriesUnfoldAggregationBuilder::registerAggregators)
        );
    }

    @Override
    public List<PipelineAggregationSpec> getPipelineAggregations() {
        return List.of(
            // Register TimeSeriesCoordinatorAggregation
            new PipelineAggregationSpec(
                TimeSeriesCoordinatorAggregationBuilder.NAME,
                TimeSeriesCoordinatorAggregationBuilder::new,
                TimeSeriesCoordinatorAggregationBuilder::parse
            ).addResultReader(InternalTimeSeries::new)
        );
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (TSDB_ENGINE_ENABLED.get(indexSettings.getSettings())) {
            return Optional.of(new TSDBEngineFactory());
        }
        return Optional.empty();
    }
}
