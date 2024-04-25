// package com.qichacha.cdc.connectors.iceberg.sink;
//
// import org.apache.flink.cdc.common.configuration.ConfigOption;
// import org.apache.flink.cdc.common.configuration.Configuration;
// import com.ververica.cdc.common.factories.DataSinkFactory;
// import com.ververica.cdc.common.sink.DataSink;
// import org.apache.iceberg.flink.CatalogLoader;
// import org.apache.iceberg.flink.FlinkCatalog;
// import org.apache.iceberg.flink.FlinkCatalogFactory;
//
// import java.lang.reflect.InvocationTargetException;
// import java.lang.reflect.Method;
// import java.time.ZoneId;
// import java.util.HashMap;
// import java.util.Map;
// import java.util.Set;
//
// import static com.qichacha.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.JDBC_URL;
// import static com.ververica.cdc.common.pipeline.PipelineOptions.PIPELINE_LOCAL_TIME_ZONE;
//
/// **
// * A {@link DataSinkFactory} to create {@link IcebergDataSink}.
// */
//
// public class IcebergDataSinkFactory implements DataSinkFactory {
//    public static final String IDENTIFIER = "iceberg";
//
//    @Override
//    public DataSink createDataSink(Context context) {
//        Configuration configuration = context.getFactoryConfiguration();
//
//        String s = configuration.get(JDBC_URL);
//        FlinkCatalogFactory factory = new FlinkCatalogFactory();
//        FlinkCatalog flinkCatalog =
//                (FlinkCatalog) factory.createCatalog(s, new HashMap<>());
//        CatalogLoader catalogLoader;
//        try {
//            Class<?> calalogClass = Class.forName(FlinkCatalog.class.getName());
//            Method getCatalogLoader = calalogClass.getMethod("getCatalogLoader");
//            getCatalogLoader.setAccessible(true);
//            catalogLoader = (CatalogLoader) getCatalogLoader.invoke(flinkCatalog);
//        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException |
//                 IllegalAccessException e) {
//            throw new RuntimeException(e);
//        }
//
//        IcebergDataSinkOptions sinkOptions =
//                buildSinkConnectorOptions(configuration);
////        TableCreateConfig tableCreateConfig =
////                TableCreateConfig.from(context.getFactoryConfiguration());
////        SchemaChangeConfig schemaChangeConfig =
////                SchemaChangeConfig.from(context.getFactoryConfiguration());
//        String zoneStr = configuration.get(PIPELINE_LOCAL_TIME_ZONE);
//        ZoneId zoneId =
//                PIPELINE_LOCAL_TIME_ZONE.defaultValue().equals(zoneStr)
//                        ? ZoneId.systemDefault()
//                        : ZoneId.of(zoneStr);
//        return new IcebergDataSink(catalogLoader, sinkOptions, zoneId);
//    }
//
//    @Override
//    public String identifier() {
//        return IDENTIFIER;
//    }
//
//    @Override
//    public Set<ConfigOption<?>> requiredOptions() {
//        return null;
//    }
//
//    @Override
//    public Set<ConfigOption<?>> optionalOptions() {
//        return null;
//    }
//
//    /**
//     * FlinkDynamicTableFactory
//     * FlinkWriteOptions
//     *
//     * @param cdcConfig
//     * @return
//     */
//    private IcebergDataSinkOptions buildSinkConnectorOptions(Configuration cdcConfig) {
//        org.apache.flink.configuration.Configuration sinkConfig =
//                new org.apache.flink.configuration.Configuration();
//        // required sink configurations
//        sinkConfig.set(JDBC_URL, cdcConfig.get(JDBC_URL));
//        sinkConfig.set(IcebergDataSinkOptions.LOAD_URL, cdcConfig.get(LOAD_URL));
//        sinkConfig.set(IcebergDataSinkOptions.USERNAME, cdcConfig.get(USERNAME));
//        sinkConfig.set(IcebergDataSinkOptions.PASSWORD, cdcConfig.get(PASSWORD));
//        // optional sink configurations
//        cdcConfig
//                .getOptional(SINK_LABEL_PREFIX)
//                .ifPresent(
//                        config -> sinkConfig.set(IcebergDataSinkOptions.SINK_LABEL_PREFIX,
// config));
//        cdcConfig
//                .getOptional(SINK_CONNECT_TIMEOUT)
//                .ifPresent(
//                        config ->
//                                sinkConfig.set(IcebergDataSinkOptions.SINK_CONNECT_TIMEOUT,
// config));
//        cdcConfig
//                .getOptional(SINK_WAIT_FOR_CONTINUE_TIMEOUT)
//                .ifPresent(
//                        config ->
//                                sinkConfig.set(
//                                        IcebergDataSinkOptions.SINK_WAIT_FOR_CONTINUE_TIMEOUT,
//                                        config));
//        cdcConfig
//                .getOptional(SINK_BATCH_MAX_SIZE)
//                .ifPresent(
//                        config -> sinkConfig.set(IcebergDataSinkOptions.SINK_BATCH_MAX_SIZE,
// config));
//        cdcConfig
//                .getOptional(SINK_BATCH_FLUSH_INTERVAL)
//                .ifPresent(
//                        config ->
//                                sinkConfig.set(
//                                        IcebergDataSinkOptions.SINK_BATCH_FLUSH_INTERVAL,
// config));
//        cdcConfig
//                .getOptional(SINK_SCAN_FREQUENCY)
//                .ifPresent(
//                        config -> sinkConfig.set(IcebergDataSinkOptions.SINK_SCAN_FREQUENCY,
// config));
//        cdcConfig
//                .getOptional(SINK_IO_THREAD_COUNT)
//                .ifPresent(
//                        config ->
//                                sinkConfig.set(IcebergDataSinkOptions.SINK_IO_THREAD_COUNT,
// config));
//        cdcConfig
//                .getOptional(SINK_AT_LEAST_ONCE_USE_TRANSACTION_LOAD)
//                .ifPresent(
//                        config ->
//                                sinkConfig.set(
//                                        IcebergDataSinkOptions
//                                                .SINK_AT_LEAST_ONCE_USE_TRANSACTION_LOAD,
//                                        config));
//        cdcConfig
//                .getOptional(SINK_METRIC_HISTOGRAM_WINDOW_SIZE)
//                .ifPresent(
//                        config ->
//                                sinkConfig.set(
//                                        IcebergDataSinkOptions.SINK_METRIC_HISTOGRAM_WINDOW_SIZE,
//                                        config));
//        // specified sink configurations for cdc scenario
//        sinkConfig.set(IcebergDataSinkOptions.DATABASE_NAME, "*");
//        sinkConfig.set(IcebergDataSinkOptions.TABLE_NAME, "*");
//        sinkConfig.set(IcebergDataSinkOptions.SINK_USE_NEW_SINK_API, true);
//        // currently cdc framework only supports at-least-once
//        sinkConfig.set(IcebergDataSinkOptions.SINK_SEMANTIC, "at-least-once");
//
//        Map<String, String> streamProperties =
//                getPrefixConfigs(cdcConfig.toMap(), SINK_PROPERTIES_PREFIX);
//        // force to use json format for stream load to simplify the configuration,
//        // such as there is no need to reconfigure the "columns" property after
//        // schema change. csv format can be supported in the future if needed
//        streamProperties.put("sink.properties.format", "json");
//        streamProperties.put("sink.properties.strip_outer_array", "true");
//        streamProperties.put("sink.properties.ignore_json_size", "true");
//
//        return new StarRocksSinkOptions(sinkConfig, streamProperties);
//    }
//
// }
