package org.apache.iceberg.flink.sink;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimestampType;
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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.types.RowKind;

import com.qichacha.cdc.connectors.iceberg.types.utils.DataTypeUtils;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.SerializableSupplier;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class IcebergSinkWriterOperator extends AbstractStreamOperator<WriteResult>
        implements OneInputStreamOperator<Event, WriteResult>, BoundedOneInput {

    private SchemaEvolutionClient schemaEvolutionClient;

    private final OperatorID schemaOperatorID;
    private final SerializableSupplier<Table> tableSupplier;
    private final RowType flinkRowType;
    private final List<Integer> equalityFieldIds;

    private final IcebergEventStreamWriter<RowData> copySinkWriter;
    private volatile Schema schema;
    private final long targetFileSizeBytes;
    private final FileFormat format;
    private final Map<String, String> writeProperties;
    private final boolean upsertMode;
    private static final Map<org.apache.flink.cdc.common.types.DataType, RecordDataGetter<?>>
            CONVERTERS = new ConcurrentHashMap<>();

    public IcebergSinkWriterOperator(
            OperatorID schemaOperatorID,
            SerializableSupplier<Table> tableSupplier,
            RowType flinkRowType,
            List<Integer> equalityFieldIds,
            long targetFileSizeBytes,
            FileFormat format,
            Map<String, String> writeProperties,
            boolean upsertMode) {
        this.schemaOperatorID = schemaOperatorID;
        this.tableSupplier = tableSupplier;
        this.flinkRowType = flinkRowType;
        this.equalityFieldIds = equalityFieldIds;
        this.targetFileSizeBytes = targetFileSizeBytes;
        this.format = format;
        this.writeProperties = writeProperties;
        this.upsertMode = upsertMode;
        this.chainingStrategy = ChainingStrategy.ALWAYS;

        // TODO restore schema 丢失
        // TODO 优先级，状态 > 目标表加载
        // this.schema =
        Table initTable = tableSupplier.get();
        TaskWriterFactory<RowData> taskWriterFactory =
                new EventTaskWriterFactory(
                        tableSupplier,
                        flinkRowType,
                        targetFileSizeBytes,
                        format,
                        writeProperties,
                        equalityFieldIds,
                        upsertMode);
        this.copySinkWriter = new IcebergEventStreamWriter<>(initTable.name(), taskWriterFactory);
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<WriteResult>> output) {
        super.setup(containingTask, config, output);
        this.copySinkWriter.setup(containingTask, config, output);
        this.schemaEvolutionClient =
                new SchemaEvolutionClient(
                        getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway(),
                        schemaOperatorID);
    }

    @Override
    public void open() throws Exception {
        super.open();
        copySinkWriter.open();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        copySinkWriter.initializeState(context);
        schemaEvolutionClient.registerSubtask(getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        copySinkWriter.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof SchemaChangeEvent) {
            LOG.info("SchemaChangeEvent {}", event);
            TableId tableId = ((SchemaChangeEvent) event).tableId();
            schemaEvolutionClient.notifyFlushSuccess(
                    getRuntimeContext().getIndexOfThisSubtask(), tableId);
        } else if (event instanceof FlushEvent) {
            TableId tableId = ((FlushEvent) event).getTableId();
            copySinkWriter.flush();
            schemaEvolutionClient.notifyFlushSuccess(
                    getRuntimeContext().getIndexOfThisSubtask(), tableId);
            LOG.debug(
                    "Notify Flush Success, SubtaskId is {}",
                    getRuntimeContext().getIndexOfThisSubtask());

        } else if (event instanceof DataChangeEvent) {
            TableId tableId = ((DataChangeEvent) event).tableId();
            if (!copySinkWriter.hasWriter()) {
                recreate(tableId);
            }
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            serializerRecord(dataChangeEvent);
        }
    }

    void refreshSchema(TableId tableId) throws Exception {
        Optional<Schema> latestSchema = schemaEvolutionClient.getLatestSchema(tableId);
        latestSchema.ifPresent(value -> schema = value);
    }

    void recreate(TableId tableId) throws Exception {
        Optional<Schema> latestSchema = schemaEvolutionClient.getLatestSchema(tableId);
        if (latestSchema.isPresent()) {
            schema = latestSchema.get();
            LOG.info("Get latest schema is {}.", schema);
            DataType dataType = DataTypeUtils.toFlinkDataType(schema.toRowDataType());

            // TODO 不能使用 SerializableTable 替换为 tableLoader
            TaskWriterFactory<RowData> taskWriterFactory =
                    new EventTaskWriterFactory(
                            tableSupplier,
                            LogicalTypeUtils.toRowType(dataType.getLogicalType()),
                            targetFileSizeBytes,
                            format,
                            writeProperties,
                            equalityFieldIds,
                            upsertMode);
            copySinkWriter.schemaEvolution(taskWriterFactory);
        } else {
            throw new RuntimeException(
                    "Could not find schema message from SchemaRegistry for " + tableId);
        }
    }

    /**
     * TODO before after partition changed situation will be not collect. Add repartition operator
     * in front of this writer
     */
    private void serializerRecord(DataChangeEvent dataChangeEvent) throws Exception {
        OperationType op = dataChangeEvent.op();
        switch (op) {
            case UPDATE:
            case REPLACE:
            case INSERT:
                RecordData after = dataChangeEvent.after();
                RowData rowData = serializerRecord(RowKind.INSERT, after);
                copySinkWriter.processElement(new StreamRecord<>(rowData));
                break;
            case DELETE:
                RowData deleteRowData = serializerRecord(RowKind.DELETE, dataChangeEvent.before());
                copySinkWriter.processElement(new StreamRecord<>(deleteRowData));
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

    private RowData serializerRecord(RowKind rowKind, RecordData recordData) {
        GenericRowData rowData = new GenericRowData(rowKind, recordData.getArity());
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
        copySinkWriter.endInput();
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
            case CHAR:
            case VARCHAR:
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

            case TIME_WITHOUT_TIME_ZONE:
                // Time in RowData is in milliseconds (Integer), while iceberg's time is
                // microseconds (Long).
                return (row, pos) -> ((long) row.getInt(pos)) * 1_000;

            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) dataType;
                return (row, pos) ->
                        org.apache.flink.table.data.TimestampData.fromEpochMillis(
                                row.getTimestamp(pos, timestampType.getPrecision())
                                        .getMillisecond());

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType lzTs = (LocalZonedTimestampType) dataType;
                return (row, pos) -> {
                    TimestampData timestampData = row.getTimestamp(pos, lzTs.getPrecision());
                    return org.apache.flink.table.data.TimestampData.fromEpochMillis(
                            timestampData.getMillisecond());
                };

            case ROW:
                org.apache.flink.cdc.common.types.RowType rowType =
                        (org.apache.flink.cdc.common.types.RowType) dataType;
                return (row, pos) -> row.getRow(pos, rowType.getFieldCount());

            default:
                return null;
        }
    }

    private static RecordDataGetter<?> wrapIntoNullableConverter(RecordDataGetter<?> converter) {
        return new RecordDataGetter<Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object get(RecordData data, int pos) {
                if (data.isNullAt(pos)) {
                    return null;
                }
                return converter.get(data, pos);
            }
        };
    }
}
