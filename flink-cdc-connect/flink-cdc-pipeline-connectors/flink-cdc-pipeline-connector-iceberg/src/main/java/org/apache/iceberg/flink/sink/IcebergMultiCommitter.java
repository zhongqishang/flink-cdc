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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Multi Committer. */
public class IcebergMultiCommitter extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<TableWriteResult, Void>, BoundedOneInput {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IcebergMultiCommitter.class);

    private final CatalogLoader catalogLoader;
    private transient Catalog catalog;
    private final boolean replacePartitions;
    private final Map<String, String> snapshotProperties;

    private final String branch;

    private final Integer workerPoolSize;
    private StreamTaskStateInitializer streamTaskStateManager;

    private static final ListStateDescriptor<TableIdentifier> TABLE_ID_DESCRIPTOR =
            new ListStateDescriptor<>(
                    "iceberg-multi-committer-table-id", TypeInformation.of(TableIdentifier.class));
    private transient ListState<TableIdentifier> tableIdListState;
    private final Set<TableIdentifier> tableIds = new HashSet<>();
    private final Map<TableIdentifier, IcebergFilesCommitter> committers = Maps.newConcurrentMap();

    IcebergMultiCommitter(
            CatalogLoader catalogLoader,
            boolean replacePartitions,
            Map<String, String> snapshotProperties,
            Integer workerPoolSize,
            String branch) {
        this.catalogLoader = catalogLoader;
        this.replacePartitions = replacePartitions;
        this.snapshotProperties = snapshotProperties;
        this.workerPoolSize = workerPoolSize;
        this.branch = branch;
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
            throws Exception {
        this.streamTaskStateManager = streamTaskStateManager;
        super.initializeState(streamTaskStateManager);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        this.tableIdListState = context.getOperatorStateStore().getListState(TABLE_ID_DESCRIPTOR);
        Iterable<TableIdentifier> tableIdentifiers = tableIdListState.get();
        for (TableIdentifier tableIdentifier : tableIdentifiers) {
            IcebergFilesCommitter committer =
                    committers.computeIfAbsent(
                            tableIdentifier, k -> createIcebergFileCommitter(tableIdentifier));
            committer.initializeState(context);
            LOG.info(
                    "IcebergFilesCommitter initializeState tableId : {}",
                    tableIdentifier.toString());
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Void>> output) {
        super.setup(containingTask, config, output);
    }

    @Override
    public void endInput() throws Exception {
        for (IcebergFilesCommitter committer : committers.values()) {
            committer.endInput();
        }
    }

    @Override
    public void processElement(StreamRecord<TableWriteResult> element) throws Exception {
        TableWriteResult tableWriteResult = element.getValue();
        TableIdentifier tableId = tableWriteResult.getTableId();
        tableIds.add(tableId);
        IcebergFilesCommitter committer =
                committers.computeIfAbsent(tableId, k -> createIcebergFileCommitter(tableId));
        StreamRecord<FlinkWriteResult> streamRecord =
                new StreamRecord<>(
                        new FlinkWriteResult(
                                tableWriteResult.getCheckpointId(),
                                tableWriteResult.getWriteResult()),
                        element.getTimestamp());
        committer.processElement(streamRecord);
    }

    private IcebergFilesCommitter createIcebergFileCommitter(TableIdentifier tableId) {
        if (catalog == null) {
            catalog = catalogLoader.loadCatalog();
        }
        Table table = catalog.loadTable(tableId);
        IcebergFilesCommitter committer =
                new IcebergFilesCommitter(
                        TableLoader.fromCatalog(catalogLoader, tableId),
                        replacePartitions,
                        snapshotProperties,
                        workerPoolSize,
                        branch,
                        table.spec());
        committer.setup(getContainingTask(), getOperatorConfig(), output);
        try {
            committer.open();
            // Method initializeState = committer.getClass().getMethod("initializeState",
            // StateInitializationContext.class);
            // initializeState.setAccessible(true);
            // initializeState.invoke(committer, context);
            committer.initializeState(streamTaskStateManager);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return committer;
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        tableIdListState.clear();
        tableIdListState.addAll(new ArrayList<>(tableIds));
        for (IcebergFilesCommitter committer : committers.values()) {
            committer.snapshotState(context);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // super.notifyCheckpointComplete(checkpointId);
        for (IcebergFilesCommitter committer : committers.values()) {
            committer.notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        for (IcebergFilesCommitter committer : committers.values()) {
            committer.close();
        }
    }
}
