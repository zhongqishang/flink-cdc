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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.runtime.partitioning.PostPartitionProcessor;
import org.apache.flink.cdc.runtime.partitioning.PrePartitionOperator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.cdc.runtime.typeutils.PartitioningEventTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.qichacha.cdc.connectors.iceberg.sink.PartitioningEventPartitioner;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.conf.FlinkMultiWriteConf;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;

import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_STRATEGY;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_LEVEL;

/** Flink Event Sink. */
public class FlinkEventSink {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkEventSink.class);

    private static final String ICEBERG_STREAM_WRITER_NAME =
            IcebergStreamWriter.class.getSimpleName();
    private static final String ICEBERG_FILES_COMMITTER_NAME =
            IcebergFilesCommitter.class.getSimpleName();

    private FlinkEventSink() {}

    /**
     * Initialize a {@link Builder} to export the data from generic input data stream into iceberg
     * table. We use {@link RowData} inside the sink connector, so users need to provide a mapper
     * function and a {@link TypeInformation} to convert those generic records to a RowData
     * DataStream.
     *
     * @param input the generic source input data stream.
     * @param mapper function to convert the generic data to {@link RowData}
     * @param outputType to define the {@link TypeInformation} for the input data.
     * @param <T> the data type of records.
     * @return {@link Builder} to connect the iceberg table.
     */
    public static <T> Builder builderFor(
            DataStream<T> input, MapFunction<T, Event> mapper, TypeInformation<Event> outputType) {
        return new Builder().forMapperOutputType(input, mapper, outputType);
    }

    /**
     * Initialize a {@link Builder} to export the data from input data stream with {@link RowData}s
     * into iceberg table.
     *
     * @param input the source input data stream with {@link RowData}s.
     * @return {@link Builder} to connect the iceberg table.
     */
    public static Builder forEvent(DataStream<Event> input) {
        return new Builder().forEvent(input);
    }

    /** FlinkEventSink Builder. */
    public static class Builder {
        private Function<String, DataStream<Event>> inputCreator = null;
        private CatalogLoader catalogLoader;
        private String uidPrefix = null;
        private OperatorID operatorID = null;
        private final Map<String, String> snapshotProperties = Maps.newHashMap();
        private ReadableConfig readableConfig = new Configuration();
        private final Map<String, String> writeOptions = Maps.newHashMap();
        private FlinkMultiWriteConf flinkWriteConf = null;

        private Builder() {}

        private Builder forEvent(DataStream<Event> newRowDataInput) {
            this.inputCreator = ignored -> newRowDataInput;
            return this;
        }

        private <T> Builder forMapperOutputType(
                DataStream<T> input,
                MapFunction<T, Event> mapper,
                TypeInformation<Event> outputType) {
            this.inputCreator =
                    newUidPrefix -> {
                        // Input stream order is crucial for some situation(e.g. in cdc case).
                        // Therefore, we
                        // need to set the parallelism
                        // of map operator same as its input to keep map operator chaining its
                        // input, and avoid
                        // rebalanced by default.
                        SingleOutputStreamOperator<Event> inputStream =
                                input.map(mapper, outputType)
                                        .setParallelism(input.getParallelism());
                        if (newUidPrefix != null) {
                            inputStream
                                    .name(operatorName(newUidPrefix))
                                    .uid(newUidPrefix + "-mapper");
                        }
                        return inputStream;
                    };
            return this;
        }

        /**
         * The table loader is used for loading tables in {@link IcebergFilesCommitter} lazily, we
         * need this loader because {@link Table} is not serializable and could not just use the
         * loaded table from Builder#table in the remote task manager.
         *
         * @param catalogLoader to load iceberg table inside tasks.
         * @return {@link Builder} to connect the iceberg table.
         */
        public Builder catalogLoader(CatalogLoader catalogLoader) {
            this.catalogLoader = catalogLoader;
            return this;
        }

        /**
         * Set the write properties for Flink sink. View the supported properties in {@link
         * FlinkWriteOptions}
         */
        public Builder set(String property, String value) {
            writeOptions.put(property, value);
            return this;
        }

        /**
         * Set the write properties for Flink sink. View the supported properties in {@link
         * FlinkWriteOptions}
         */
        public Builder setAll(Map<String, String> properties) {
            writeOptions.putAll(properties);
            return this;
        }

        public Builder overwrite(boolean newOverwrite) {
            writeOptions.put(
                    FlinkWriteOptions.OVERWRITE_MODE.key(), Boolean.toString(newOverwrite));
            return this;
        }

        public Builder flinkConf(ReadableConfig config) {
            this.readableConfig = config;
            return this;
        }

        /**
         * Configure the write {@link DistributionMode} that the flink sink will use. Currently,
         * flink support {@link DistributionMode#NONE} and {@link DistributionMode#HASH}.
         *
         * @param mode to specify the write distribution mode.
         * @return {@link Builder} to connect the iceberg table.
         */
        public Builder distributionMode(DistributionMode mode) {
            Preconditions.checkArgument(
                    !DistributionMode.RANGE.equals(mode),
                    "Flink does not support 'range' write distribution mode now.");
            if (mode != null) {
                writeOptions.put(FlinkWriteOptions.DISTRIBUTION_MODE.key(), mode.modeName());
            }
            return this;
        }

        /**
         * Configuring the write parallel number for iceberg stream writer.
         *
         * @param newWriteParallelism the number of parallel iceberg stream writer.
         * @return {@link Builder} to connect the iceberg table.
         */
        public Builder writeParallelism(int newWriteParallelism) {
            writeOptions.put(
                    FlinkWriteOptions.WRITE_PARALLELISM.key(),
                    Integer.toString(newWriteParallelism));
            return this;
        }

        /**
         * All INSERT/UPDATE_AFTER events from input stream will be transformed to UPSERT events,
         * which means it will DELETE the old records and then INSERT the new records. In
         * partitioned table, the partition fields should be a subset of equality fields, otherwise
         * the old row that located in partition-A could not be deleted by the new row that located
         * in partition-B.
         *
         * @param enabled indicate whether it should transform all INSERT/UPDATE_AFTER events to
         *     UPSERT.
         * @return {@link Builder} to connect the iceberg table.
         */
        public Builder upsert(boolean enabled) {
            writeOptions.put(
                    FlinkWriteOptions.WRITE_UPSERT_ENABLED.key(), Boolean.toString(enabled));
            return this;
        }

        /**
         * Set the uid prefix for FlinkSink operators. Note that FlinkSink internally consists of
         * multiple operators (like writer, committer, dummy sink etc.) Actually operator uid will
         * be appended with a suffix like "uidPrefix-writer". <br>
         * <br>
         * If provided, this prefix is also applied to operator names. <br>
         * <br>
         * Flink auto generates operator uid if not set explicitly. It is a recommended <a
         * href="https://ci.apache.org/projects/flink/flink-docs-master/docs/ops/production_ready/">
         * best-practice to set uid for all operators</a> before deploying to production. Flink has
         * an option to {@code pipeline.auto-generate-uid=false} to disable auto-generation and
         * force explicit setting of all operator uid. <br>
         * <br>
         * Be careful with setting this for an existing job, because now we are changing the
         * operator uid from an auto-generated one to this new value. When deploying the change with
         * a checkpoint, Flink won't be able to restore the previous Flink sink operator state (more
         * specifically the committer operator state). You need to use {@code
         * --allowNonRestoredState} to ignore the previous sink state. During restore Flink sink
         * state is used to check if last commit was actually successful or not. {@code
         * --allowNonRestoredState} can lead to data loss if the Iceberg commit failed in the last
         * completed checkpoint.
         *
         * @param newPrefix prefix for Flink sink operator uid and name
         * @return {@link Builder} to connect the iceberg table.
         */
        public Builder uidPrefix(String newPrefix) {
            this.uidPrefix = newPrefix;
            return this;
        }

        public Builder operatorID(OperatorID operatorID) {
            this.operatorID = operatorID;
            return this;
        }

        public Builder setSnapshotProperties(Map<String, String> properties) {
            snapshotProperties.putAll(properties);
            return this;
        }

        public Builder setSnapshotProperty(String property, String value) {
            snapshotProperties.put(property, value);
            return this;
        }

        public Builder toBranch(String branch) {
            writeOptions.put(FlinkWriteOptions.BRANCH.key(), branch);
            return this;
        }

        private <T> DataStreamSink<T> chainIcebergOperators() {
            Preconditions.checkArgument(
                    inputCreator != null,
                    "Please use forRowData() or forMapperOutputType() to initialize the input DataStream.");
            Preconditions.checkNotNull(catalogLoader, "Table loader shouldn't be null");

            DataStream<Event> eventDataStream = inputCreator.apply(uidPrefix);

            flinkWriteConf = new FlinkMultiWriteConf(writeOptions, readableConfig);

            // Add parallel writers that append rows to files
            SingleOutputStreamOperator<TableWriteResult> writerStream =
                    appendWriter(eventDataStream);

            // Add single-parallelism committer that commits files
            // after successful checkpoint or end of input
            SingleOutputStreamOperator<Void> committerStream = appendCommitter(writerStream);

            // Add dummy discard sink
            return appendDummySink(committerStream);
        }

        /**
         * Append the iceberg sink operators to write records to iceberg table.
         *
         * @return {@link DataStreamSink} for sink.
         */
        public DataStreamSink<Void> append() {
            return chainIcebergOperators();
        }

        private String operatorName(String suffix) {
            return uidPrefix != null ? uidPrefix + "-" + suffix : suffix;
        }

        @SuppressWarnings("unchecked")
        private <T> DataStreamSink<T> appendDummySink(
                SingleOutputStreamOperator<Void> committerStream) {
            DataStreamSink<T> resultStream =
                    committerStream
                            .addSink(new DiscardingSink())
                            .name(operatorName("IcebergSink Multi Sink"))
                            .setParallelism(1);
            if (uidPrefix != null) {
                resultStream = resultStream.uid(uidPrefix + "-dummysink");
            }
            return resultStream;
        }

        private SingleOutputStreamOperator<Void> appendCommitter(
                SingleOutputStreamOperator<TableWriteResult> writerStream) {
            IcebergMultiCommitter filesCommitters =
                    new IcebergMultiCommitter(
                            catalogLoader,
                            flinkWriteConf.overwriteMode(),
                            snapshotProperties,
                            flinkWriteConf.workerPoolSize(),
                            flinkWriteConf.branch());
            SingleOutputStreamOperator<Void> committerStream =
                    writerStream
                            .transform(
                                    operatorName(ICEBERG_FILES_COMMITTER_NAME),
                                    Types.VOID,
                                    filesCommitters)
                            .setParallelism(1)
                            .setMaxParallelism(1);
            if (uidPrefix != null) {
                committerStream = committerStream.uid(uidPrefix + "-committer");
            }
            return committerStream;
        }

        private SingleOutputStreamOperator<TableWriteResult> appendWriter(DataStream<Event> input) {

            int parallelism =
                    flinkWriteConf.writeParallelism() == null
                            ? input.getParallelism()
                            : flinkWriteConf.writeParallelism();

            DataStream<Event> distributeStream =
                    input.transform(
                                    "IcebergPrePartition",
                                    new PartitioningEventTypeInfo(),
                                    new PrePartitionOperator(operatorID, input.getParallelism()))
                            .setParallelism(input.getParallelism())
                            .partitionCustom(
                                    new PartitioningEventPartitioner(),
                                    new EventEqualityFieldKeySelector(catalogLoader))
                            .map(new PostPartitionProcessor(), new EventTypeInfo())
                            .name("PostPartition");

            SingleOutputStreamOperator<TableWriteResult> writerStream =
                    distributeStream
                            .transform(
                                    operatorName(ICEBERG_STREAM_WRITER_NAME),
                                    TypeInformation.of(TableWriteResult.class),
                                    new IcebergSinkWriterOperator(
                                            operatorID,
                                            catalogLoader,
                                            flinkWriteConf.targetDataFileSize(),
                                            flinkWriteConf.dataFileFormat(),
                                            writeProperties(
                                                    flinkWriteConf.dataFileFormat(),
                                                    flinkWriteConf),
                                            flinkWriteConf.upsertMode()))
                            .setParallelism(parallelism);

            if (uidPrefix != null) {
                writerStream = writerStream.uid(uidPrefix + "-writer");
            }
            return writerStream;
        }

        static RowType toFlinkRowType(Schema schema, TableSchema requestedSchema) {
            if (requestedSchema != null) {
                // Convert the flink schema to iceberg schema firstly, then reassign ids to match
                // the
                // existing iceberg schema.
                Schema writeSchema =
                        TypeUtil.reassignIds(FlinkSchemaUtil.convert(requestedSchema), schema);

                TypeUtil.validateWriteSchema(schema, writeSchema, true, true);

                // We use this flink schema to read values from RowData. The flink's TINYINT and
                // SMALLINT will be promoted to iceberg INTEGER, that means if we use iceberg's
                // table
                // schema to read TINYINT (backendby 1 'byte'), we will read 4 bytes rather than 1
                // byte,
                // it will mess up the byte array in BinaryRowData. So here we must use flink
                // schema.
                return (RowType) requestedSchema.toRowDataType().getLogicalType();
            } else {
                return FlinkSchemaUtil.convert(schema);
            }
        }

        /**
         * Based on the {@link FileFormat} overwrites the table level compression properties for the
         * table write.
         *
         * @param format The FileFormat to use
         * @param conf The write configuration
         * @return The properties to use for writing
         */
        protected static Map<String, String> writeProperties(
                FileFormat format, FlinkMultiWriteConf conf) {
            // Todo
            // common properties
            Map<String, String> writeProperties = Maps.newHashMap();

            switch (format) {
                case PARQUET:
                    writeProperties.put(PARQUET_COMPRESSION, conf.parquetCompressionCodec());
                    String parquetCompressionLevel = conf.parquetCompressionLevel();
                    if (parquetCompressionLevel != null) {
                        writeProperties.put(PARQUET_COMPRESSION_LEVEL, parquetCompressionLevel);
                    }

                    break;
                case AVRO:
                    writeProperties.put(AVRO_COMPRESSION, conf.avroCompressionCodec());
                    String avroCompressionLevel = conf.avroCompressionLevel();
                    if (avroCompressionLevel != null) {
                        writeProperties.put(AVRO_COMPRESSION_LEVEL, conf.avroCompressionLevel());
                    }

                    break;
                case ORC:
                    writeProperties.put(ORC_COMPRESSION, conf.orcCompressionCodec());
                    writeProperties.put(ORC_COMPRESSION_STRATEGY, conf.orcCompressionStrategy());
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format("Unknown file format %s", format));
            }

            return writeProperties;
        }
    }
}
