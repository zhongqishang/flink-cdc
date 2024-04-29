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

package org.apache.iceberg.flink.sink;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Iceberg Sink Writer Operator. */
public class IcebergSinkWriterOperator extends AbstractStreamOperator<TableWriteResult>
        implements OneInputStreamOperator<Event, TableWriteResult>, BoundedOneInput {

    private final CatalogLoader catalogLoader;
    private transient Catalog catalog;

    private SchemaEvolutionClient schemaEvolutionClient;

    private final OperatorID schemaOperatorID;
    private final Map<TableId, Schema> schemas = new ConcurrentHashMap<>();
    private final Map<TableId, Table> tables = new ConcurrentHashMap<>();
    private final Map<TableId, IcebergEventStreamWriter<RowData>> writes =
            new ConcurrentHashMap<>();
    private final long targetFileSizeBytes;
    private final FileFormat format;
    private final Map<String, String> writeProperties;
    private final boolean upsertMode;
    private static final Map<org.apache.flink.cdc.common.types.DataType, RecordDataGetter<?>>
            CONVERTERS = new ConcurrentHashMap<>();

    public IcebergSinkWriterOperator(
            OperatorID schemaOperatorID,
            CatalogLoader catalogLoader,
            long targetFileSizeBytes,
            FileFormat format,
            Map<String, String> writeProperties,
            boolean upsertMode) {
        this.schemaOperatorID = schemaOperatorID;
        this.catalogLoader = catalogLoader;
        this.targetFileSizeBytes = targetFileSizeBytes;
        this.format = format;
        this.writeProperties = writeProperties;
        this.upsertMode = upsertMode;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<TableWriteResult>> output) {
        super.setup(containingTask, config, output);
        this.schemaEvolutionClient =
                new SchemaEvolutionClient(
                        getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway(),
                        schemaOperatorID);
    }

    @Override
    public void open() throws Exception {
        super.open();
        catalog = catalogLoader.loadCatalog();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        for (IcebergEventStreamWriter<RowData> copySinkWriter : writes.values()) {
            copySinkWriter.initializeState(context);
        }
        schemaEvolutionClient.registerSubtask(getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        for (IcebergEventStreamWriter<RowData> copySinkWriter : writes.values()) {
            copySinkWriter.prepareSnapshotPreBarrier(checkpointId);
        }
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof SchemaChangeEvent) {
            LOG.info("SchemaChangeEvent {}", event);
            TableId tableId = ((SchemaChangeEvent) event).tableId();
            if (event instanceof CreateTableEvent) {
                CreateTableEvent createTableEvent = (CreateTableEvent) event;
                schemas.put(createTableEvent.tableId(), createTableEvent.getSchema());
            } else {
                SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
                schemas.put(
                        schemaChangeEvent.tableId(),
                        SchemaUtils.applySchemaChangeEvent(
                                schemas.get(schemaChangeEvent.tableId()), schemaChangeEvent));
                IcebergEventStreamWriter<RowData> remove = writes.remove(tableId);
                if (remove != null) {
                    remove.flush();
                    remove.close();
                }
            }
            schemaEvolutionClient.notifyFlushSuccess(
                    getRuntimeContext().getIndexOfThisSubtask(), tableId);
        } else if (event instanceof FlushEvent) {
            TableId tableId = ((FlushEvent) event).getTableId();
            IcebergEventStreamWriter<RowData> streamWriter = writes.get(tableId);
            if (streamWriter == null) {
                LOG.warn("No IcebergEventStreamWriter found for table {}", tableId);
            } else {
                streamWriter.flush();
            }
            schemaEvolutionClient.notifyFlushSuccess(
                    getRuntimeContext().getIndexOfThisSubtask(), tableId);
            LOG.info(
                    "Notify Flush Success, SubtaskId is {}",
                    getRuntimeContext().getIndexOfThisSubtask());

        } else if (event instanceof DataChangeEvent) {
            TableId tableId = ((DataChangeEvent) event).tableId();
            IcebergEventStreamWriter<RowData> streamWriter =
                    writes.computeIfAbsent(tableId, k -> createCopySinkWriter(tableId));
            if (!streamWriter.hasWriter()) {
                recreate(tableId);
            }
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            serializerRecord(streamWriter, dataChangeEvent);
        }
    }

    private IcebergEventStreamWriter<RowData> createCopySinkWriter(TableId tableId) {
        IcebergEventStreamWriter<RowData> streamWriter =
                new IcebergEventStreamWriter<>(TableIdentifier.parse(tableId.identifier()));
        streamWriter.setTaskWriterFactory(createTaskWriterFactory(tableId));
        streamWriter.setup(getContainingTask(), getOperatorConfig(), output);
        streamWriter.open();
        return streamWriter;
    }

    private TaskWriterFactory<RowData> createTaskWriterFactory(TableId tableId) {
        Table table =
                tables.computeIfAbsent(
                        tableId,
                        t -> catalog.loadTable(TableIdentifier.parse(tableId.identifier())));
        table.refresh();
        List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().identifierFieldIds());

        return new EventTaskWriterFactory(
                table,
                FlinkSchemaUtil.convert(table.schema()),
                targetFileSizeBytes,
                format,
                writeProperties,
                equalityFieldIds,
                upsertMode);
    }

    void recreate(TableId tableId) throws Exception {
        Schema schema = schemas.get(tableId);
        LOG.info("Recreate writer schema is {}.", schema);
        writes.get(tableId).setTaskWriterFactory(createTaskWriterFactory(tableId));
    }

    /**
     * TODO before after partition changed situation will be not collect. Add repartition operator
     * in front of this writer
     */
    private void serializerRecord(
            IcebergEventStreamWriter<RowData> streamWriter, DataChangeEvent dataChangeEvent)
            throws Exception {
        OperationType op = dataChangeEvent.op();
        TableId tableId = dataChangeEvent.tableId();
        switch (op) {
            case UPDATE:
            case REPLACE:
            case INSERT:
                // TODO validate schema
                // Kafka multiple partitions duo to not in strict order
                RecordData after = dataChangeEvent.after();
                RowData rowData = serializerRecord(tableId, RowKind.INSERT, after);
                streamWriter.processElement(new StreamRecord<>(rowData));
                break;
            case DELETE:
                RowData deleteRowData =
                        serializerRecord(tableId, RowKind.DELETE, dataChangeEvent.before());
                streamWriter.processElement(new StreamRecord<>(deleteRowData));
                break;
            default:
                throw new UnsupportedOperationException("Unsupported Operation " + op);
        }
    }

    private RecordDataGetter<?> getOrCreateConverter(
            org.apache.flink.cdc.common.types.DataType type) {
        return CONVERTERS.computeIfAbsent(type, this::createConverter);
    }

    /** Creates a runtime converter which is null safe. */
    private RecordDataGetter<?> createConverter(org.apache.flink.cdc.common.types.DataType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    private RowData serializerRecord(TableId tableId, RowKind rowKind, RecordData recordData) {
        GenericRowData rowData = new GenericRowData(rowKind, recordData.getArity());
        // TODO 不能解决 failover 的问题，需要引入 lastSchema 和 routeRef
        if (!schemas.containsKey(tableId)) {
            throw new RuntimeException("Table " + tableId + " does not exist");
        }
        Schema schema = schemas.get(tableId);
        List<org.apache.flink.cdc.common.types.DataType> columnDataTypes =
                schema.getColumnDataTypes();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            RecordDataGetter<?> converter = getOrCreateConverter(columnDataTypes.get(i));
            Object o = converter.get(recordData, i);
            rowData.setField(i, o);
        }
        return rowData;
    }

    @Override
    public void endInput() throws Exception {
        for (IcebergEventStreamWriter<RowData> writer : writes.values()) {
            writer.endInput();
        }
    }

    private interface RecordDataGetter<T> {
        T get(RecordData data, int pos);
    }

    private RecordDataGetter<?> createNotNullConverter(
            org.apache.flink.cdc.common.types.DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case TINYINT:
                return RecordData::getByte;
            case SMALLINT:
                return RecordData::getShort;
            case INTEGER:
                return RecordData::getInt;
            case BIGINT:
                return RecordData::getLong;
            case FLOAT:
                return RecordData::getFloat;
            case DOUBLE:
                return RecordData::getDouble;
            case BOOLEAN:
                return RecordData::getBoolean;
            case CHAR:
            case VARCHAR:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (row, pos) -> StringData.fromString(row.getString(pos).toString());
            case BINARY:
            case VARBINARY:
                return RecordData::getBinary;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                return (row, pos) ->
                        DecimalData.fromBigDecimal(
                                row.getDecimal(pos, precision, scale).toBigDecimal(),
                                precision,
                                scale);
                // case TIME_WITHOUT_TIME_ZONE:
                //     // Time in RowData is in milliseconds (Integer), while iceberg's time is
                //     // microseconds (Long).
                //     return (row, pos) -> ((long) row.getInt(pos)) * 1_000;
                // case TIMESTAMP_WITHOUT_TIME_ZONE:
                //     TimestampType timestampType = (TimestampType) dataType;
                //     return (row, pos) ->
                //             org.apache.flink.table.data.TimestampData.fromEpochMillis(
                //                     row.getTimestamp(pos, timestampType.getPrecision())
                //                             .getMillisecond());
                // case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                //     LocalZonedTimestampType lzTs = (LocalZonedTimestampType) dataType;
                //     return (row, pos) -> {
                //         TimestampData timestampData = row.getTimestamp(pos, lzTs.getPrecision());
                //         return org.apache.flink.table.data.TimestampData.fromEpochMillis(
                //                 timestampData.getMillisecond());
                //     };
                // case ARRAY:
            case ROW:
                org.apache.flink.cdc.common.types.RowType rowType =
                        (org.apache.flink.cdc.common.types.RowType) dataType;
                return (row, pos) -> row.getRow(pos, rowType.getFieldCount());

            default:
                return null;
        }
    }

    private static RecordDataGetter<?> wrapIntoNullableConverter(RecordDataGetter<?> converter) {
        return (RecordDataGetter<Object>)
                (data, pos) -> {
                    if (data.isNullAt(pos)) {
                        return null;
                    }
                    return converter.get(data, pos);
                };
    }
}
