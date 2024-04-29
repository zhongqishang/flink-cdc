/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.qichacha.cdc.connectors.iceberg;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.composer.flink.coordination.OperatorIDGenerator;
import org.apache.flink.cdc.composer.flink.translator.SchemaOperatorTranslator;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.mysql.source.MySqlDataSource;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import com.qichacha.cdc.connectors.iceberg.sink.CatalogPropertiesUtils;
import com.qichacha.cdc.connectors.iceberg.sink.IcebergMetadataApplier;
import com.qichacha.cdc.connectors.iceberg.types.utils.FlinkCdcSchemaUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkEventSink;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCHEMA_CHANGE_ENABLED;

/** Sync TiDB. */
public class SyncTiDBTest {
    public static void main(String[] args) throws Exception {

        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10 * 1000L);
        env.disableOperatorChaining();

        String database = "manage_financial_db";
        String tableName = "comp_prelist_tutoring_report";
        int parallelism = env.getParallelism();

        Properties properties = new Properties();
        properties.setProperty("useSSL", "false");

        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname("172.18.104.217")
                        .port(4000)
                        .username("tidev")
                        .password("J50VUNgvdNgj65Bnst1I")
                        .databaseList(database)
                        .tableList(String.format("%s.%s", database, tableName))
                        .serverId(getServerId(env.getParallelism()))
                        .serverTimeZone("UTC+8")
                        .includeSchemaChanges(SCHEMA_CHANGE_ENABLED.defaultValue())
                        .startupOptions(StartupOptions.snapshot())
                        .skipSnapshotBackfill(true)
                        .jdbcProperties(properties);

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) new MySqlDataSource(configFactory).getEventSourceProvider();

        MetadataAccessor metadataAccessor =
                new MySqlDataSource(configFactory).getMetadataAccessor();

        DataStreamSource<Event> source =
                env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        MySqlDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo());

        // Schema operator
        SchemaOperatorTranslator schemaOperatorTranslator =
                new SchemaOperatorTranslator(
                        SchemaChangeBehavior.EVOLVE,
                        PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID.defaultValue(),
                        DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT);

        org.apache.hadoop.conf.Configuration hadoopConf = FlinkCatalogFactory.clusterHadoopConf();

        Map<String, String> catalogMap = CatalogPropertiesUtils.getProperties("ods_iceberg");
        CatalogLoader catalogLoader = CatalogLoader.hive("hive", hadoopConf, catalogMap);
        TableIdentifier tableIdentifier = TableIdentifier.of("ods_iceberg", tableName);
        Catalog catalog = catalogLoader.loadCatalog();

        Schema icebergSchema;

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);
        if (catalog.tableExists(tableIdentifier)) {
            // Load schema from iceberg
            tableLoader.open();
            Table table = tableLoader.loadTable();
            icebergSchema = table.schema();
        } else {
            // Load schema from mysql and create iceberg
            org.apache.flink.cdc.common.schema.Schema tableSchema =
                    metadataAccessor.getTableSchema(TableId.tableId(database, tableName));
            icebergSchema = FlinkCdcSchemaUtil.convert(tableSchema);
            // Default set non partition
            catalog.createTable(tableIdentifier, icebergSchema, null, catalogMap);
        }

        DataStream<Event> stream =
                schemaOperatorTranslator.translate(
                        source,
                        parallelism,
                        new IcebergMetadataApplier(catalogLoader),
                        Collections.emptyList());

        OperatorIDGenerator schemaOperatorIDGenerator =
                new OperatorIDGenerator(schemaOperatorTranslator.getSchemaOperatorUid());

        ArrayList<String> equalityColumns =
                Lists.newArrayList(icebergSchema.identifierFieldNames());
        TableSchema tableSchema = FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(icebergSchema));

        // Add sink
        FlinkEventSink.forEvent(stream)
                .operatorID(schemaOperatorIDGenerator.generate())
                .catalogLoader(catalogLoader)
                .overwrite(false)
                .set("table-refresh-interval", "10s")
                .append();

        env.execute(String.format("Flink CDC sync %s", tableIdentifier));
    }

    public static String getServerId(int parallelism) {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + parallelism);
    }
}
