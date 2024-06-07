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

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.util.CollectionUtil;

import java.util.List;
import java.util.stream.Collectors;

/** Copy from DataTypeUtils. */
public class DataTypeUtils {
    /**
     * Convert CDC's {@link DataType} to Flink's internal {@link
     * org.apache.flink.table.types.DataType}.
     */
    @Deprecated
    public static org.apache.flink.table.types.DataType toFlinkDataType(DataType type) {
        // ordered by type root definition
        List<DataType> children = type.getChildren();
        int length = DataTypes.getLength(type).orElse(0);
        int precision = DataTypes.getPrecision(type).orElse(0);
        int scale = DataTypes.getScale(type).orElse(0);
        switch (type.getTypeRoot()) {
            case CHAR:
                return org.apache.flink.table.api.DataTypes.CHAR(length);
            case VARCHAR:
                return org.apache.flink.table.api.DataTypes.VARCHAR(length);
            case BOOLEAN:
                return org.apache.flink.table.api.DataTypes.BOOLEAN();
            case BINARY:
                return org.apache.flink.table.api.DataTypes.BINARY(length);
            case VARBINARY:
                return org.apache.flink.table.api.DataTypes.VARBINARY(length);
            case DECIMAL:
                return org.apache.flink.table.api.DataTypes.DECIMAL(precision, scale);
            case TINYINT:
                return org.apache.flink.table.api.DataTypes.TINYINT();
            case SMALLINT:
                return org.apache.flink.table.api.DataTypes.SMALLINT();
            case INTEGER:
                return org.apache.flink.table.api.DataTypes.INT();
            case DATE:
                return org.apache.flink.table.api.DataTypes.DATE();
            case TIME_WITHOUT_TIME_ZONE:
                return org.apache.flink.table.api.DataTypes.TIME(precision);
            case BIGINT:
                return org.apache.flink.table.api.DataTypes.BIGINT();
            case FLOAT:
                return org.apache.flink.table.api.DataTypes.FLOAT();
            case DOUBLE:
                return org.apache.flink.table.api.DataTypes.DOUBLE();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                // Iceberg unsupported type TIMESTAMP_WITH_TIME_ZONE
                // return org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_TIME_ZONE(precision);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(
                        precision);
            case ARRAY:
                Preconditions.checkState(children != null && !children.isEmpty());
                return org.apache.flink.table.api.DataTypes.ARRAY(toFlinkDataType(children.get(0)));
            case MAP:
                Preconditions.checkState(children != null && children.size() > 1);
                return org.apache.flink.table.api.DataTypes.MAP(
                        toFlinkDataType(children.get(0)), toFlinkDataType(children.get(1)));
            case ROW:
                Preconditions.checkState(!CollectionUtil.isNullOrEmpty(children));
                RowType rowType = (RowType) type;
                List<org.apache.flink.table.api.DataTypes.Field> fields =
                        rowType.getFields().stream()
                                .map(
                                        dataField ->
                                                org.apache.flink.table.api.DataTypes.FIELD(
                                                        dataField.getName(),
                                                        toFlinkDataType(dataField.getType())))
                                .collect(Collectors.toList());
                return org.apache.flink.table.api.DataTypes.ROW(fields);
            default:
                throw new IllegalArgumentException("Illegal type: " + type);
        }
    }

    /**
     * Convert CDC's {@link DataType} to Flink's internal {@link
     * org.apache.flink.table.types.DataType}.
     */
    public static org.apache.flink.table.types.DataType toFlinkQccDataType(DataType type) {
        // ordered by type root definition
        List<DataType> children = type.getChildren();
        int length = DataTypes.getLength(type).orElse(0);
        int precision = DataTypes.getPrecision(type).orElse(0);
        int scale = DataTypes.getScale(type).orElse(0);
        switch (type.getTypeRoot()) {
            case CHAR:
                return org.apache.flink.table.api.DataTypes.CHAR(length);
            case VARCHAR:
                return org.apache.flink.table.api.DataTypes.VARCHAR(length);
            case BOOLEAN:
                return org.apache.flink.table.api.DataTypes.STRING();
            case BINARY:
                return org.apache.flink.table.api.DataTypes.BINARY(length);
            case VARBINARY:
                return org.apache.flink.table.api.DataTypes.VARBINARY(length);
            case DECIMAL:
                return org.apache.flink.table.api.DataTypes.DECIMAL(precision, scale);
            case TINYINT:
                return org.apache.flink.table.api.DataTypes.TINYINT();
            case SMALLINT:
                return org.apache.flink.table.api.DataTypes.SMALLINT();
            case INTEGER:
                return org.apache.flink.table.api.DataTypes.INT();
            case DATE:
                return org.apache.flink.table.api.DataTypes.STRING();
            case TIME_WITHOUT_TIME_ZONE:
                return org.apache.flink.table.api.DataTypes.STRING();
            case BIGINT:
                return org.apache.flink.table.api.DataTypes.BIGINT();
            case FLOAT:
                return org.apache.flink.table.api.DataTypes.FLOAT();
            case DOUBLE:
                return org.apache.flink.table.api.DataTypes.DOUBLE();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                // Iceberg unsupported type TIMESTAMP_WITH_TIME_ZONE
                // return org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_TIME_ZONE(precision);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return org.apache.flink.table.api.DataTypes.STRING();
            case ARRAY:
                Preconditions.checkState(children != null && !children.isEmpty());
                return org.apache.flink.table.api.DataTypes.ARRAY(
                        toFlinkQccDataType(children.get(0)));
            case MAP:
                Preconditions.checkState(children != null && children.size() > 1);
                return org.apache.flink.table.api.DataTypes.MAP(
                        toFlinkQccDataType(children.get(0)), toFlinkQccDataType(children.get(1)));
            case ROW:
                Preconditions.checkState(!CollectionUtil.isNullOrEmpty(children));
                RowType rowType = (RowType) type;
                List<org.apache.flink.table.api.DataTypes.Field> fields =
                        rowType.getFields().stream()
                                .map(
                                        dataField ->
                                                org.apache.flink.table.api.DataTypes.FIELD(
                                                        dataField.getName(),
                                                        toFlinkQccDataType(dataField.getType())))
                                .collect(Collectors.toList());
                return org.apache.flink.table.api.DataTypes.ROW(fields);
            default:
                throw new IllegalArgumentException("Illegal type: " + type);
        }
    }
}
