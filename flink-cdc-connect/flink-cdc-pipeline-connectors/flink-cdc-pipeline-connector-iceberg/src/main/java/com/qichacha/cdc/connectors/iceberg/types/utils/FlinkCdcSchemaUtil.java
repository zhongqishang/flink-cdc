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

/** Copy from org.apache.iceberg.flink.FlinkSchemaUtil. */
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
        Type converted = rowType.accept(new FlinkCdcTypeToTypeString(rowType));
        Schema icebergSchema = new Schema(converted.asStructType().fields());
        if (schema.primaryKeys() != null && !schema.primaryKeys().isEmpty()) {
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
