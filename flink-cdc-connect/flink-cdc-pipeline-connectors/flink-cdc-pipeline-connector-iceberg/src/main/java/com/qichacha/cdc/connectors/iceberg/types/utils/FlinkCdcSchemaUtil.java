package com.qichacha.cdc.connectors.iceberg.types.utils;

import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;

import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Set;

/** Copy from org.apache.iceberg.flink.FlinkSchemaUtil */
public class FlinkCdcSchemaUtil {

    /** Convert the flink table schema to apache iceberg schema with column comment. */
    public static Schema convert(org.apache.flink.cdc.common.schema.Schema schema) {
        List<Column> tableColumns = schema.getColumns();
        // copy from org.apache.flink.table.api.Schema#toRowDataType
        DataField[] fields =
                tableColumns.stream()
                        .map(
                                column ->
                                        DataTypes.FIELD(
                                                column.getName(),
                                                column.getType(),
                                                column.getComment()))
                        .toArray(DataField[]::new);

        RowType rowType = DataTypes.ROW(fields);
        Type converted = rowType.accept(new FlinkCdcTypeToType(rowType));
        Schema icebergSchema = new Schema(converted.asStructType().fields());
        if (schema.primaryKeys() != null && schema.primaryKeys().size() > 0) {
            return freshIdentifierFieldIds(icebergSchema, schema.primaryKeys());
        } else {
            return icebergSchema;
        }
    }

    private static Schema freshIdentifierFieldIds(Schema icebergSchema, List<String> primaryKeys) {
        // Locate the identifier field id list.
        Set<Integer> identifierFieldIds = Sets.newHashSet();
        for (String primaryKey : primaryKeys) {
            Types.NestedField field = icebergSchema.findField(primaryKey);
            Preconditions.checkNotNull(
                    field,
                    "Cannot find field ID for the primary key column %s in schema %s",
                    primaryKey,
                    icebergSchema);
            identifierFieldIds.add(field.fieldId());
        }
        return new Schema(
                icebergSchema.schemaId(), icebergSchema.asStruct().fields(), identifierFieldIds);
    }
}
