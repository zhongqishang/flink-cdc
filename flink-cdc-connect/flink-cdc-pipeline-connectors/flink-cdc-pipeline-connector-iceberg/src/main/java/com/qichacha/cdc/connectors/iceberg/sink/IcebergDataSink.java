// package com.qichacha.cdc.connectors.iceberg.sink;
//
// import com.ververica.cdc.common.event.Event;
// import com.ververica.cdc.common.sink.DataSink;
// import com.ververica.cdc.common.sink.DataStreamSinkProvider;
// import com.ververica.cdc.common.sink.EventSinkProvider;
// import com.ververica.cdc.common.sink.MetadataApplier;
// import org.apache.flink.api.common.typeinfo.TypeInformation;
// import org.apache.flink.streaming.api.datastream.DataStream;
// import org.apache.flink.streaming.api.datastream.DataStreamSink;
// import org.apache.flink.table.data.RowData;
// import org.apache.iceberg.catalog.Catalog;
// import org.apache.iceberg.catalog.TableIdentifier;
// import org.apache.iceberg.flink.CatalogLoader;
// import org.apache.iceberg.flink.TableLoader;
// import org.apache.iceberg.flink.sink.FlinkEventSink;
//
// import java.io.Serializable;
// import java.time.ZoneId;
//
/// **
// * A {@link DataSink} for Iceberg connector that supports schema evolution.
// */
// public class IcebergDataSink implements DataSink, Serializable {
//
//    private static final long serialVersionUID = 1L;
//    private final CatalogLoader catalogLoader;
//
//    /**
//     * TODO remove
//     * Configurations for sink connector.
//     */
//    private final IcebergDataSinkOptions sinkOptions;
//    /**
//     * The local time zone used when converting from <code>TIMESTAMP WITH LOCAL TIME ZONE</code>.
//     */
//    private final ZoneId zoneId;
//
//    public IcebergDataSink(CatalogLoader catalogLoader, IcebergDataSinkOptions sinkOptions, ZoneId
// zoneId) {
//        this.catalogLoader = catalogLoader;
//        this.sinkOptions = sinkOptions;
//        this.zoneId = zoneId;
//    }
//
//    @Override
//    public EventSinkProvider getEventSinkProvider() {
//        Catalog catalog = catalogLoader.loadCatalog();
//        TableLoader tableLoader = TableLoader.fromCatalog(
//                catalogLoader, TableIdentifier.of("catalogDatabase", "catalogTable"));
//
//        return (DataStreamSinkProvider) dataStream -> FlinkEventSink.builderFor(dataStream, event
// -> null, TypeInformation.of(RowData.class))
//                .tableLoader(tableLoader)
//                .tableSchema(tableSchema)
//                .equalityFieldColumns(equalityColumns)
//                .overwrite(overwrite)
//                .setAll(writeProps)
//                .flinkConf(readableConfig)
//                .append();
//    }
//
//    private DataStreamSink<Void> createDataStreamSink(DataStream<Event> dataStream) {
//
//        DataStreamSink<Void> dataStreamSink = null;
//
//        // Flink
//        return dataStreamSink;
//    }
//
//
//    @Override
//    public MetadataApplier getMetadataApplier() {
//        return new IcebergMetadataApplier(catalogLoader, true);
//    }
// }
