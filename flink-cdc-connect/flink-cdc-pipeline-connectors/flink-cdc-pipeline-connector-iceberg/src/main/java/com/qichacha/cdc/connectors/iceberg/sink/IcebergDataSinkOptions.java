/// *
// * Copyright 2023 Ververica Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package com.qichacha.cdc.connectors.iceberg.sink;
//
// import org.apache.flink.cdc.common.configuration.ConfigOption;
// import com.ververica.cdc.common.configuration.ConfigOptions;
//
// import java.time.Duration;
// import java.util.List;
//
/// ** Options for {@link IcebergDataSink}. */
// public class IcebergDataSinkOptions {
//
//    // ------------------------------------------------------------------------------------------
//    // Options for sink connector
//    // ------------------------------------------------------------------------------------------
//
//    public static final ConfigOption<String> JDBC_URL =
//            ConfigOptions.key("jdbc-url")
//                    .stringType()
//                    .noDefaultValue()
//                    .withDescription(
//                            "Url of the jdbc like:
// `jdbc:mysql://fe_ip1:query_port,fe_ip2:query_port...`.");
//
//    public static final ConfigOption<List<String>> LOAD_URL =
//            ConfigOptions.key("load-url")
//                    .stringType()
//                    .asList()
//                    .noDefaultValue()
//                    .withDescription(
//                            "Url of the stream load, if you you don't specify the http/https
// prefix, the default http. "
//                                    + "like:
// `fe_ip1:http_port;http://fe_ip2:http_port;https://fe_nlb`.");
//
//    public static final ConfigOption<String> USERNAME =
//            ConfigOptions.key("username")
//                    .stringType()
//                    .noDefaultValue()
//                    .withDescription("StarRocks user name.");
//
//    public static final ConfigOption<String> PASSWORD =
//            ConfigOptions.key("password")
//                    .stringType()
//                    .noDefaultValue()
//                    .withDescription("StarRocks user password.");
//
//    public static final ConfigOption<String> SINK_LABEL_PREFIX =
//            ConfigOptions.key("sink.label-prefix")
//                    .stringType()
//                    .noDefaultValue()
//                    .withDescription(
//                            "The prefix of the stream load label. Available values are within
// [-_A-Za-z0-9]");
//
//    public static final ConfigOption<Integer> SINK_CONNECT_TIMEOUT =
//            ConfigOptions.key("sink.connect.timeout-ms")
//                    .intType()
//                    .defaultValue(30000)
//                    .withDescription("Timeout in millisecond for connecting to the `load-url`.");
//
//    public static final ConfigOption<Integer> SINK_WAIT_FOR_CONTINUE_TIMEOUT =
//            ConfigOptions.key("sink.wait-for-continue.timeout-ms")
//                    .intType()
//                    .defaultValue(30000)
//                    .withDescription(
//                            "Timeout in millisecond to wait for 100-continue response from FE http
// server.");
//
//    public static final ConfigOption<Long> SINK_BATCH_MAX_SIZE =
//            ConfigOptions.key("sink.buffer-flush.max-bytes")
//                    .longType()
//                    .defaultValue(150L * 1024 * 1024)
//                    .withDescription("Max data bytes of the flush.");
//
//    public static final ConfigOption<Long> SINK_BATCH_FLUSH_INTERVAL =
//            ConfigOptions.key("sink.buffer-flush.interval-ms")
//                    .longType()
//                    .defaultValue(300000L)
//                    .withDescription("Flush interval of the row batch in millisecond.");
//
//    public static final ConfigOption<Long> SINK_SCAN_FREQUENCY =
//            ConfigOptions.key("sink.scan-frequency.ms")
//                    .longType()
//                    .defaultValue(50L)
//                    .withDescription(
//                            "Scan frequency in milliseconds to check whether the buffer reaches
// the flush interval.");
//
//    public static final ConfigOption<Integer> SINK_IO_THREAD_COUNT =
//            ConfigOptions.key("sink.io.thread-count")
//                    .intType()
//                    .defaultValue(2)
//                    .withDescription(
//                            "Number of threads used for concurrent stream loads among different
// tables.");
//
//    public static final ConfigOption<Boolean> SINK_AT_LEAST_ONCE_USE_TRANSACTION_LOAD =
//            ConfigOptions.key("sink.at-least-once.use-transaction-stream-load")
//                    .booleanType()
//                    .defaultValue(true)
//                    .withDescription(
//                            "Whether to use transaction stream load for at-least-once when it's
// available.");
//
//    public static final ConfigOption<Integer> SINK_METRIC_HISTOGRAM_WINDOW_SIZE =
//            ConfigOptions.key("sink.metric.histogram-window-size")
//                    .intType()
//                    .defaultValue(100)
//                    .withDescription("Window size of histogram metrics.");
//
//    // ------------------------------------------------------------------------------------------
//    // Options for schema change
//    // ------------------------------------------------------------------------------------------
//
//    /**
//     * The prefix for properties used for creating a table. You can refer to StarRocks
// documentation
//     * for the DDL.
//     * https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/#create-a-table
//     */
//    public static final String TABLE_CREATE_PROPERTIES_PREFIX = "table.create.properties.";
//
//    public static final ConfigOption<Integer> TABLE_CREATE_NUM_BUCKETS =
//            ConfigOptions.key("table.create.num-buckets")
//                    .intType()
//                    .noDefaultValue()
//                    .withDescription(
//                            "Number of buckets for creating a StarRocks table. If not set,
// StarRocks will "
//                                    + "automatically choose the number of buckets.");
//
//    public static final ConfigOption<Duration> TABLE_SCHEMA_CHANGE_TIMEOUT =
//            ConfigOptions.key("table.schema-change.timeout")
//                    .durationType()
//                    .defaultValue(Duration.ofSeconds(1800))
//                    .withDescription(
//                            "Timeout for a schema change on StarRocks side, and must be an
// integral multiple of "
//                                    + "seconds. StarRocks will cancel the schema change after
// timeout which will "
//                                    + "cause the sink failure.");
// }
