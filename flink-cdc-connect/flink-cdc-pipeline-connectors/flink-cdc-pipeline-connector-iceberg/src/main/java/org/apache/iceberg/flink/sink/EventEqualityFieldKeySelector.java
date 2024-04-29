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
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.StructProjection;

import java.util.Map;
import java.util.Set;

/**
 * Create a {@link KeySelector} to shuffle by equality fields, to ensure same equality fields record
 * will be emitted to same writer in order.
 */
public class EventEqualityFieldKeySelector implements KeySelector<PartitioningEvent, Integer> {

    private final CatalogLoader catalogLoader;
    private transient Catalog catalog;

    private transient Map<TableId, RecordDataWrapper> rowDataWrappers;
    private transient Map<TableId, StructProjection> structProjections;
    private transient Map<TableId, StructLikeWrapper> structLikeWrappers;

    EventEqualityFieldKeySelector(CatalogLoader catalogLoader) {
        this.catalogLoader = catalogLoader;
    }

    /**
     * Construct the {@link RecordDataWrapper} lazily here because few members in it are not
     * serializable. In this way, we don't have to serialize them with forcing.
     */
    protected RecordDataWrapper lazyRowDataWrapper(TableId tableId) {
        if (catalog == null) {
            catalog = catalogLoader.loadCatalog();
        }
        if (rowDataWrappers == null) {
            rowDataWrappers = Maps.newConcurrentMap();
        }
        if (structProjections == null) {
            structProjections = Maps.newConcurrentMap();
        }
        if (structLikeWrappers == null) {
            structLikeWrappers = Maps.newConcurrentMap();
        }
        RecordDataWrapper rowDataWrapper = rowDataWrappers.get(tableId);
        if (rowDataWrapper == null) {
            Table table = catalog.loadTable(TableIdentifier.parse(tableId.identifier()));
            Set<Integer> integers = table.schema().identifierFieldIds();
            rowDataWrapper =
                    new RecordDataWrapper(
                            FlinkSchemaUtil.convert(table.schema()), table.schema().asStruct());
            rowDataWrappers.put(tableId, rowDataWrapper);

            // Construct the {@link StructProjection} lazily because it is not serializable.
            Schema deleteSchema = TypeUtil.select(table.schema(), integers);
            StructProjection structProjection =
                    StructProjection.create(table.schema(), deleteSchema);
            structProjections.put(tableId, structProjection);
            StructLikeWrapper structLikeWrapper =
                    StructLikeWrapper.forType(deleteSchema.asStruct());
            structLikeWrappers.put(tableId, structLikeWrapper);
        }
        return rowDataWrapper;
    }

    /** Construct the {@link StructProjection} lazily because it is not serializable. */
    protected StructProjection lazyStructProjection(TableId tableId) {
        if (!structProjections.containsKey(tableId)) {
            throw new RuntimeException(
                    "Cannot find structProjection for table " + tableId.identifier());
        }
        return structProjections.get(tableId);
    }

    /** Construct the {@link StructLikeWrapper} lazily because it is not serializable. */
    protected StructLikeWrapper lazyStructLikeWrapper(TableId tableId) {
        if (!structLikeWrappers.containsKey(tableId)) {
            throw new RuntimeException(
                    "Cannot find structLikeWrapper for table " + tableId.identifier());
        }
        return structLikeWrappers.get(tableId);
    }

    @Override
    public Integer getKey(PartitioningEvent event) {
        Event payload = event.getPayload();
        int targetPartition = event.getTargetPartition();
        if (payload instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) payload;
            RecordData recordData =
                    dataChangeEvent.op() == OperationType.DELETE
                            ? dataChangeEvent.before()
                            : dataChangeEvent.after();
            RecordDataWrapper wrappedRowData =
                    lazyRowDataWrapper(dataChangeEvent.tableId()).wrap(recordData);
            StructProjection projectedRowData =
                    lazyStructProjection(dataChangeEvent.tableId()).wrap(wrappedRowData);
            StructLikeWrapper wrapper =
                    lazyStructLikeWrapper(dataChangeEvent.tableId()).set(projectedRowData);
            return Math.abs(wrapper.hashCode());
        } else {
            // FlushEvent
            // SchemaChangeEvent
            return targetPartition;
        }
    }
}
