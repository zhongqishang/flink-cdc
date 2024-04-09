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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.table.types.logical.RowType;

import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.StructProjection;

import java.util.List;

/**
 * Create a {@link KeySelector} to shuffle by equality fields, to ensure same equality fields record
 * will be emitted to same writer in order.
 */
class EventEqualityFieldKeySelector implements KeySelector<PartitioningEvent, Integer> {

    private final Schema schema;
    private final RowType flinkSchema;
    private final Schema deleteSchema;

    private transient RecordDataWrapper rowDataWrapper;
    private transient StructProjection structProjection;
    private transient StructLikeWrapper structLikeWrapper;

    EventEqualityFieldKeySelector(
            Schema schema, RowType flinkSchema, List<Integer> equalityFieldIds) {
        this.schema = schema;
        this.flinkSchema = flinkSchema;
        this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
    }

    /**
     * Construct the {@link RecordDataWrapper} lazily here because few members in it are not
     * serializable. In this way, we don't have to serialize them with forcing.
     */
    protected RecordDataWrapper lazyRowDataWrapper() {
        if (rowDataWrapper == null) {
            rowDataWrapper = new RecordDataWrapper(flinkSchema, schema.asStruct());
        }
        return rowDataWrapper;
    }

    /** Construct the {@link StructProjection} lazily because it is not serializable. */
    protected StructProjection lazyStructProjection() {
        if (structProjection == null) {
            structProjection = StructProjection.create(schema, deleteSchema);
        }
        return structProjection;
    }

    /** Construct the {@link StructLikeWrapper} lazily because it is not serializable. */
    protected StructLikeWrapper lazyStructLikeWrapper() {
        if (structLikeWrapper == null) {
            structLikeWrapper = StructLikeWrapper.forType(deleteSchema.asStruct());
        }
        return structLikeWrapper;
    }

    @Override
    public Integer getKey(PartitioningEvent event) {
        Event payload = event.getPayload();
        int targetPartition = event.getTargetPartition();
        if (payload instanceof DataChangeEvent) {
            RecordDataWrapper wrappedRowData =
                    lazyRowDataWrapper().wrap(((DataChangeEvent) payload).after());
            StructProjection projectedRowData = lazyStructProjection().wrap(wrappedRowData);
            StructLikeWrapper wrapper = lazyStructLikeWrapper().set(projectedRowData);
            return wrapper.hashCode();
        } else {
            // FlushEvent
            // SchemaChangeEvent
            return targetPartition;
        }
    }
}
