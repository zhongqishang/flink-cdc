package com.qichacha.cdc.connectors.iceberg.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import com.qichacha.cdc.connectors.iceberg.types.utils.DataTypeUtils;
import com.qichacha.cdc.connectors.iceberg.types.utils.FlinkCdcSchemaUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.qichacha.cdc.connectors.iceberg.sink.Sync.toPartitionSpec;

/** A {@code MetadataApplier} that applies metadata changes to Iceberg. */
public class IcebergMetadataApplier implements MetadataApplier {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IcebergMetadataApplier.class);
    private final CatalogLoader catalogLoader;
    private transient Catalog catalog;
    private boolean isOpened = false;

    public IcebergMetadataApplier(CatalogLoader catalogLoader) {
        this.catalogLoader = catalogLoader;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        if (!isOpened) {
            catalog = catalogLoader.loadCatalog();
            isOpened = true;
        }

        // TODO DDL 去重操作
        if (schemaChangeEvent instanceof CreateTableEvent) {
            applyCreateTable((CreateTableEvent) schemaChangeEvent);
        } else if (schemaChangeEvent instanceof AddColumnEvent) {
            applyAddColumn((AddColumnEvent) schemaChangeEvent);
        } else if (schemaChangeEvent instanceof DropColumnEvent) {
            applyDropColumn((DropColumnEvent) schemaChangeEvent);
        } else if (schemaChangeEvent instanceof RenameColumnEvent) {
            applyRenameColumn((RenameColumnEvent) schemaChangeEvent);
        } else if (schemaChangeEvent instanceof AlterColumnTypeEvent) {
            applyAlterColumn((AlterColumnTypeEvent) schemaChangeEvent);
        } else {
            throw new UnsupportedOperationException(
                    "StarRocksDataSink doesn't support schema change event " + schemaChangeEvent);
        }
    }

    private void applyCreateTable(CreateTableEvent createTableEvent) {
        TableId tableId = createTableEvent.tableId();
        Schema schema = createTableEvent.getSchema();
        TableIdentifier tableIdentifier = TableIdentifier.of("ods_iceberg", tableId.getTableName());
        if (catalog.tableExists(tableIdentifier)) {
            // TODO 校验 schema 兼容？
            return;
        }

        org.apache.iceberg.Schema icebergSchema = FlinkCdcSchemaUtil.convert(schema);
        PartitionSpec spec =
                toPartitionSpec(
                        Lists.newArrayList(icebergSchema.identifierFieldNames()), icebergSchema);
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

        properties.put("catalog-database", "ods_iceberg");
        properties.put("catalog-table", tableId.getTableName());
        properties.put("uri", "thrift://localhost:9083");
        properties.put("hive-conf-dir", "/opt/hadoop-2.10.2/etc/hadoop");
        properties.put("write-format", "parquet");
        properties.put("write.upsert.enabled", "true");
        properties.put("overwrite-enabled", "false");

        try {
            catalog.createTable(tableIdentifier, icebergSchema, spec, properties.build());
        } catch (AlreadyExistsException e) {
            LOG.warn("Failed to apply add column, event: {}", createTableEvent, e);
        }
        LOG.info("Successful to apply add column, event: {}", createTableEvent);
    }

    private void applyAddColumn(AddColumnEvent addColumnEvent) {
        TableId tableId = addColumnEvent.tableId();
        TableIdentifier tableIdentifier = TableIdentifier.of("ods_iceberg", tableId.getTableName());
        Table loadTable = catalog.loadTable(tableIdentifier);
        Transaction transaction = loadTable.newTransaction();
        UpdateSchema pendingUpdate = transaction.updateSchema();

        for (AddColumnEvent.ColumnWithPosition columnWithPosition :
                addColumnEvent.getAddedColumns()) {
            // we will ignore position information, and always add the column to the last.
            // The reason is that ...
            Column column = columnWithPosition.getAddColumn();
            Type icebergType =
                    FlinkSchemaUtil.convert(
                            DataTypeUtils.toFlinkDataType(column.getType()).getLogicalType());
            pendingUpdate.addColumn(column.getName(), icebergType);
        }
        pendingUpdate.commit();
        transaction.commitTransaction();
        LOG.info("Successful to apply add column, event: {}", addColumnEvent);
    }

    private void applyDropColumn(DropColumnEvent dropColumnEvent) {
        TableId tableId = dropColumnEvent.tableId();
        TableIdentifier tableIdentifier = TableIdentifier.of("ods_iceberg", tableId.getTableName());
        Table loadTable = catalog.loadTable(tableIdentifier);
        Transaction transaction = loadTable.newTransaction();
        UpdateSchema pendingUpdate = transaction.updateSchema();

        List<String> dropColumns = dropColumnEvent.getDroppedColumnNames();
        for (String dropColumn : dropColumns) {
            pendingUpdate.deleteColumn(dropColumn);
        }
        pendingUpdate.commit();
        transaction.commitTransaction();
        LOG.info("Successful to apply drop column, event: {}", dropColumnEvent);
    }

    private void applyRenameColumn(RenameColumnEvent renameColumnEvent) {
        TableId tableId = renameColumnEvent.tableId();
        TableIdentifier tableIdentifier = TableIdentifier.of("ods_iceberg", tableId.getTableName());
        Table loadTable = catalog.loadTable(tableIdentifier);
        Transaction transaction = loadTable.newTransaction();
        UpdateSchema pendingUpdate = transaction.updateSchema();

        for (Map.Entry<String, String> renameColumn :
                renameColumnEvent.getNameMapping().entrySet()) {
            pendingUpdate.renameColumn(renameColumn.getKey(), renameColumn.getValue());
        }
        pendingUpdate.commit();
        transaction.commitTransaction();
        LOG.info("Successful to apply rename column, event: {}", renameColumnEvent);
    }

    private void applyAlterColumn(AlterColumnTypeEvent alterColumnTypeEvent) {
        TableId tableId = alterColumnTypeEvent.tableId();
        TableIdentifier tableIdentifier = TableIdentifier.of("ods_iceberg", tableId.getTableName());
        Table loadTable = catalog.loadTable(tableIdentifier);
        Transaction transaction = loadTable.newTransaction();
        UpdateSchema pendingUpdate = transaction.updateSchema();

        for (Map.Entry<String, DataType> renameColumn :
                alterColumnTypeEvent.getTypeMapping().entrySet()) {
            String columnName = renameColumn.getKey();
            Type icebergType =
                    FlinkSchemaUtil.convert(
                            DataTypeUtils.toFlinkDataType(renameColumn.getValue())
                                    .getLogicalType());
            pendingUpdate.updateColumn(columnName, icebergType.asPrimitiveType());
            pendingUpdate.makeColumnOptional(columnName);
        }
        pendingUpdate.commit();
        transaction.commitTransaction();
        LOG.info("Successful to apply alter column, event: {}", alterColumnTypeEvent);
    }
}
