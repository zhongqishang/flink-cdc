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
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;

import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

import java.util.Map;

/**
 * Create a {@link KeySelector} to shuffle by partition key, then each partition/bucket will be
 * written by only one task. That will reduce lots of small files in partitioned fanout write policy
 * for {@link FlinkSink}.
 */
public class EventPartitionKeySelector implements KeySelector<PartitioningEvent, Integer> {
    private final CatalogLoader catalogLoader;
    private transient Catalog catalog;
    private final transient Map<TableId, PartitionKey> partitionKeys = Maps.newConcurrentMap();
    private final transient Map<TableId, RecordDataWrapper> rowDataWrappers =
            Maps.newConcurrentMap();

    EventPartitionKeySelector(CatalogLoader catalogLoader) {
        this.catalogLoader = catalogLoader;
    }

    /**
     * Construct the {@link RecordDataWrapper} lazily here because few members in it are not
     * serializable. In this way, we don't have to serialize them with forcing.
     */
    private RecordDataWrapper lazyRowDataWrapper(TableId tableId) {
        RecordDataWrapper rowDataWrapper = rowDataWrappers.get(tableId);
        if (rowDataWrapper == null) {
            if (catalog == null) {
                catalog = catalogLoader.loadCatalog();
            }
            Table table = catalog.loadTable(TableIdentifier.parse(tableId.identifier()));
            rowDataWrapper =
                    new RecordDataWrapper(
                            FlinkSchemaUtil.convert(table.schema()), table.schema().asStruct());
            rowDataWrappers.put(tableId, rowDataWrapper);
            partitionKeys.put(tableId, new PartitionKey(table.spec(), table.schema()));
        }
        return rowDataWrapper;
    }

    @Override
    public Integer getKey(PartitioningEvent event) {
        Event payload = event.getPayload();
        // TODO partition changed will be cause data duplicate.
        if (payload instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) payload;
            TableId tableId = dataChangeEvent.tableId();
            RecordDataWrapper wrapped = lazyRowDataWrapper(tableId).wrap(dataChangeEvent.after());
            PartitionKey partitionKey = partitionKeys.get(tableId);
            partitionKey.partition(wrapped);
            return partitionKey.toPath().hashCode();
        } else {
            // FlushEvent
            // SchemaChangeEvent
            return event.getTargetPartition();
        }
    }
}
