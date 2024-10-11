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

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Iceberg Sink Writer Operator. */
public class IcebergSinkWriterOperator extends AbstractStreamOperator<TableWriteResult>
        implements OneInputStreamOperator<Event, TableWriteResult>, BoundedOneInput {

    protected static final Logger LOG = LoggerFactory.getLogger(IcebergSinkWriterOperator.class);
    private final CatalogLoader catalogLoader;
    private transient Catalog catalog;

    private SchemaEvolutionClient schemaEvolutionClient;

    private final OperatorID schemaOperatorID;
    private final Map<TableId, Schema> schemasCache = new ConcurrentHashMap<>();
    private final Map<TableId, Table> tables = new ConcurrentHashMap<>();
    private final Map<TableId, IcebergEventStreamWriter<RowData>> writes =
            new ConcurrentHashMap<>();
    private final long targetFileSizeBytes;
    private final FileFormat format;
    private final Map<String, String> writeProperties;
    private final boolean upsertMode;
    private static final Map<org.apache.flink.cdc.common.types.DataType, RecordDataGetter<?>>
            CONVERTERS = new ConcurrentHashMap<>();
    private long schemaChangeCheckpointId = Integer.MIN_VALUE;

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
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof SchemaChangeEvent) {
            LOG.info("SchemaChangeEvent {}", event);
            TableId tableId = ((SchemaChangeEvent) event).tableId();
            if (event instanceof CreateTableEvent) {
                CreateTableEvent createTableEvent = (CreateTableEvent) event;
                schemasCache.put(createTableEvent.tableId(), createTableEvent.getSchema());
            } else if (event instanceof TruncateTableEvent) {
                output.collect(
                        new StreamRecord<>(
                                new TableWriteResult(
                                        0,
                                        TableIdentifier.parse(tableId.identifier()),
                                        null,
                                        true)));
            } else {
                SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
                checkLatestSchema(tableId);
                Schema applySchemaChangeEvent =
                        SchemaUtils.applySchemaChangeEvent(
                                schemasCache.get(tableId), schemaChangeEvent);
                schemasCache.put(schemaChangeEvent.tableId(), applySchemaChangeEvent);
            }
        } else if (event instanceof FlushEvent) {
            TableId tableId = ((FlushEvent) event).getTableId();
            IcebergEventStreamWriter<RowData> streamWriter = writes.remove(tableId);
            if (streamWriter == null) {
                LOG.warn("No IcebergEventStreamWriter found for table {}", tableId);
            } else {
                LOG.warn(
                        "IcebergEventStreamWriter flush with previous checkpointId {}",
                        schemaChangeCheckpointId);
                streamWriter.flush(schemaChangeCheckpointId++);
                streamWriter.close();
            }
            schemaEvolutionClient.notifyFlushSuccess(
                    getRuntimeContext().getIndexOfThisSubtask(), tableId);
            LOG.info(
                    "Notify Flush Success, SubtaskId is {}",
                    getRuntimeContext().getIndexOfThisSubtask());
        } else if (event instanceof DataChangeEvent) {
            TableId tableId = ((DataChangeEvent) event).tableId();
            checkLatestSchema(tableId);
            IcebergEventStreamWriter<RowData> streamWriter =
                    writes.computeIfAbsent(tableId, k -> createCopySinkWriter(tableId));
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            try {
                serializerRecord(streamWriter, dataChangeEvent);
            } catch (ClassCastException e) {
                LOG.error("Schema : {}, row data : {}", schemasCache.get(tableId), dataChangeEvent);
                throw e;
            }
        }
    }

    private void checkLatestSchema(TableId tableId) throws Exception {
        if (!schemasCache.containsKey(tableId)) {
            Optional<Schema> latestSchema = schemaEvolutionClient.getLatestSchema(tableId);
            latestSchema.ifPresent(schema -> schemasCache.put(tableId, schema));
        }
    }

    private IcebergEventStreamWriter<RowData> createCopySinkWriter(TableId tableId) {
        LOG.info("Create Sink writer tableId : {}", tableId);
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
                RecordData after = dataChangeEvent.after();
                // TODO Kafka multiple partitions duo to not in strict order
                // validate schema
                if (schemasCache.get(tableId).getColumnCount() != after.getArity()) {
                    LOG.warn(
                            "Row data count not equal, schema column count is {}, row data column count is {}",
                            schemasCache.get(tableId).getColumnCount(),
                            after.getArity());
                }
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
        if (!schemasCache.containsKey(tableId)) {
            throw new RuntimeException("Table " + tableId + " does not exist");
        }
        // TODO 应该以 iceberg schema 为准
        org.apache.iceberg.Schema icebergSchema = tables.get(tableId).schema();
        Schema schema = schemasCache.get(tableId);
        List<org.apache.flink.cdc.common.types.DataType> columnDataTypes =
                schema.getColumnDataTypes();

        int size = icebergSchema.columns().size();
        GenericRowData rowData = new GenericRowData(rowKind, size);
        for (int i = 0; i < size; i++) {
            if (columnDataTypes.size() <= i) {
                rowData.setField(i, null);
                continue;
            }
            DataType dataType = columnDataTypes.get(i);
            RecordDataGetter<?> converter = getOrCreateConverter(dataType);
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
            case INTEGER:
                return RecordData::getInt;
            case SMALLINT:
            case TINYINT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case BOOLEAN:
            case DECIMAL:
                return (row, pos) -> StringData.fromString(row.getString(pos).toString());
            case BINARY:
            case VARBINARY:
                return RecordData::getBinary;
            case ARRAY:
                return RecordData::getArray;
            case ROW:
                org.apache.flink.cdc.common.types.RowType rowType =
                        (org.apache.flink.cdc.common.types.RowType) dataType;
                return (row, pos) -> row.getRow(pos, rowType.getFieldCount());

            default:
                throw new UnsupportedOperationException(
                        "Unsupported " + dataType.getTypeRoot().toString());
        }
    }

    private static RecordDataGetter<?> wrapIntoNullableConverter(RecordDataGetter<?> converter) {
        return (RecordDataGetter<Object>)
                (data, pos) -> {
                    // Add data.getArity() < pos
                    if (data.getArity() <= pos || data.isNullAt(pos)) {
                        return null;
                    }
                    return converter.get(data, pos);
                };
    }
}
