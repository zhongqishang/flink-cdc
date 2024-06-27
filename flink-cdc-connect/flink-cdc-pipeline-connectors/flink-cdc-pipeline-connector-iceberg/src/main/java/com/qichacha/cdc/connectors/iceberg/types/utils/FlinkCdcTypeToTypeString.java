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

import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeDefaultVisitor;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.MapType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimeType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.common.types.VarBinaryType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;

import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.stream.Collectors;

/** exclude int type, others is return String type. */
class FlinkCdcTypeToTypeString extends DataTypeDefaultVisitor<Type> {

    private final RowType root;
    private int nextId;

    FlinkCdcTypeToTypeString() {
        this.root = null;
    }

    private int getNextId() {
        int next = nextId;
        nextId += 1;
        return next;
    }

    FlinkCdcTypeToTypeString(RowType root) {
        this.root = root;
        // the root struct's fields use the first ids
        this.nextId = root.getFieldCount();
    }

    @Override
    public Type visit(CharType charType) {
        return Types.StringType.get();
    }

    @Override
    public Type visit(VarCharType varCharType) {
        return Types.StringType.get();
    }

    /** Boolean -> String. */
    @Override
    public Type visit(BooleanType booleanType) {
        return Types.StringType.get();
    }

    @Override
    public Type visit(BinaryType binaryType) {
        return Types.FixedType.ofLength(binaryType.getLength());
    }

    @Override
    public Type visit(VarBinaryType bytesType) {
        return Types.BinaryType.get();
    }

    @Override
    public Type visit(DecimalType decimalType) {
        return Types.StringType.get();
    }

    @Override
    public Type visit(TinyIntType tinyIntType) {
        return Types.StringType.get();
    }

    @Override
    public Type visit(SmallIntType smallIntType) {
        return Types.StringType.get();
    }

    @Override
    public Type visit(IntType intType) {
        return Types.IntegerType.get();
    }

    @Override
    public Type visit(BigIntType bigIntType) {
        return Types.StringType.get();
    }

    @Override
    public Type visit(FloatType floatType) {
        return Types.StringType.get();
    }

    @Override
    public Type visit(DoubleType doubleType) {
        return Types.StringType.get();
    }

    @Override
    public Type visit(DateType dateType) {
        return Types.StringType.get();
    }

    @Override
    public Type visit(TimeType timeType) {
        return Types.StringType.get();
    }

    @Override
    public Type visit(TimestampType timestampType) {
        return Types.StringType.get();
    }

    @Override
    public Type visit(ZonedTimestampType zonedTimestampType) {
        return Types.StringType.get();
    }

    @Override
    public Type visit(LocalZonedTimestampType localZonedTimestampType) {
        return Types.StringType.get();
    }

    @Override
    public Type visit(ArrayType arrayType) {
        Type elementType = arrayType.getElementType().accept(this);
        if (arrayType.getElementType().isNullable()) {
            return Types.ListType.ofOptional(getNextId(), elementType);
        } else {
            return Types.ListType.ofRequired(getNextId(), elementType);
        }
    }

    @Override
    public Type visit(MapType mapType) {
        // keys in map are not allowed to be null.
        Type keyType = mapType.getKeyType().accept(this);
        Type valueType = mapType.getValueType().accept(this);
        if (mapType.getValueType().isNullable()) {
            return Types.MapType.ofOptional(getNextId(), getNextId(), keyType, valueType);
        } else {
            return Types.MapType.ofRequired(getNextId(), getNextId(), keyType, valueType);
        }
    }

    @Override
    public Type visit(RowType rowType) {
        List<Types.NestedField> newFields =
                Lists.newArrayListWithExpectedSize(rowType.getFieldCount());
        boolean isRoot = root == rowType;

        List<Type> types =
                rowType.getFields().stream()
                        .map(f -> f.getType().accept(this))
                        .collect(Collectors.toList());

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            int id = isRoot ? i : getNextId();

            DataField field = rowType.getFields().get(i);
            String name = field.getName();
            String comment = field.getDescription();

            if (field.getType().isNullable()) {
                newFields.add(Types.NestedField.optional(id, name, types.get(i), comment));
            } else {
                newFields.add(Types.NestedField.required(id, name, types.get(i), comment));
            }
        }

        return Types.StructType.of(newFields);
    }

    @Override
    protected Type defaultMethod(DataType dataType) {
        throw new UnsupportedOperationException("Unsupported CDC data type " + dataType);
    }
}
