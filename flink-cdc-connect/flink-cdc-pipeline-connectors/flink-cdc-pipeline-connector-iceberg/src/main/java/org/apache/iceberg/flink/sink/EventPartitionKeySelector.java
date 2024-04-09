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

import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

/**
 * Create a {@link KeySelector} to shuffle by partition key, then each partition/bucket will be
 * written by only one task. That will reduce lots of small files in partitioned fanout write policy
 * for {@link FlinkSink}.
 */
public class EventPartitionKeySelector implements KeySelector<PartitioningEvent, Integer> {

    private final Schema schema;
    private final PartitionKey partitionKey;
    private final RowType flinkSchema;

    private transient RecordDataWrapper rowDataWrapper;

    EventPartitionKeySelector(PartitionSpec spec, Schema schema, RowType flinkSchema) {
        this.schema = schema;
        this.partitionKey = new PartitionKey(spec, schema);
        this.flinkSchema = flinkSchema;
    }

    /**
     * Construct the {@link RecordDataWrapper} lazily here because few members in it are not
     * serializable. In this way, we don't have to serialize them with forcing.
     */
    private RecordDataWrapper lazyRowDataWrapper() {
        if (rowDataWrapper == null) {
            rowDataWrapper = new RecordDataWrapper(flinkSchema, schema.asStruct());
        }
        return rowDataWrapper;
    }

    @Override
    public Integer getKey(PartitioningEvent event) {
        Event payload = event.getPayload();
        if (payload instanceof DataChangeEvent) {
            // TODO partition changed will be cause data duplicate.
            partitionKey.partition(lazyRowDataWrapper().wrap(((DataChangeEvent) payload).after()));
            return partitionKey.toPath().hashCode();
        } else {
            // FlushEvent
            // SchemaChangeEvent
            return event.getTargetPartition();
        }
    }
}
