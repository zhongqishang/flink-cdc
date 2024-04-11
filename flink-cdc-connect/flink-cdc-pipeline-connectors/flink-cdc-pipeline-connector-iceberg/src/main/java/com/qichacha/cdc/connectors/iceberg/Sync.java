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
import org.apache.iceberg.PartitionSpec;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCHEMA_CHANGE_ENABLED;

public class Sync {
    public static void main(String[] args) throws Exception {

        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10 * 1000L);
        env.disableOperatorChaining();

        int parallelism = env.getParallelism();

        Properties properties = new Properties();
        properties.setProperty("useSSL", "false");
        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("123456")
                        .databaseList("test")
                        .tableList("test\\.products")
                        .startupOptions(StartupOptions.initial())
                        .serverId(getServerId(env.getParallelism()))
                        .serverTimeZone("UTC")
                        .includeSchemaChanges(SCHEMA_CHANGE_ENABLED.defaultValue())
                        .startupOptions(StartupOptions.initial())
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
                        PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID.defaultValue());

        org.apache.hadoop.conf.Configuration hadoopConf = FlinkCatalogFactory.clusterHadoopConf();

        Map<String, String> catalogMap = CatalogPropertiesUtils.getProperties("ods_iceberg");
        CatalogLoader catalogLoader = CatalogLoader.hive("hive", hadoopConf, catalogMap);
        TableIdentifier tableIdentifier = TableIdentifier.of("ods_iceberg", "products");
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
                    metadataAccessor.getTableSchema(TableId.tableId("test", "products"));
            icebergSchema = FlinkCdcSchemaUtil.convert(tableSchema);
            // Default set non partition
            catalog.createTable(tableIdentifier, icebergSchema, null, catalogMap);
        }

        DataStream<Event> stream =
                schemaOperatorTranslator.translate(
                        source, parallelism, new IcebergMetadataApplier(catalogLoader));

        OperatorIDGenerator schemaOperatorIDGenerator =
                new OperatorIDGenerator(schemaOperatorTranslator.getSchemaOperatorUid());

        ArrayList<String> equalityColumns =
                Lists.newArrayList(icebergSchema.identifierFieldNames());
        TableSchema tableSchema = FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(icebergSchema));

        // Add sink
        FlinkEventSink.forEvent(stream)
                .operatorID(schemaOperatorIDGenerator.generate())
                .catalogLoader(tableLoader)
                .tableSchema(tableSchema)
                .equalityFieldColumns(equalityColumns)
                .overwrite(false)
                .set("table-refresh-interval", "10s")
                // .setAll(writeProps)
                // .flinkConf(readableConfig)
                .append();

        env.execute(String.format("Flink CDC sync %s", tableIdentifier));
    }

    public static String getServerId(int parallelism) {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + parallelism);
    }

    public static PartitionSpec toPartitionSpec(List<String> partitionKeys, Schema icebergSchema) {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
        partitionKeys.forEach(builder::identity);
        return builder.build();
    }
}
