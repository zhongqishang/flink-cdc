/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.source.config;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import com.ververica.cdc.debezium.table.DebeziumChangelogMode;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.MONGODB_SCHEME;

/** Configurations for {@link com.ververica.cdc.connectors.mongodb.source.MongoDBSource}. */
public class MongoDBSourceOptions {

    public static final ConfigOption<String> SCHEME =
            ConfigOptions.key("scheme")
                    .stringType()
                    .defaultValue(MONGODB_SCHEME)
                    .withDescription(
                            "The protocol connected to MongoDB. eg. mongodb or mongodb+srv. "
                                    + "The +srv indicates to the client that the hostname that follows corresponds to a DNS SRV record. Defaults to mongodb.");

    public static final ConfigOption<String> HOSTS =
            ConfigOptions.key("hosts")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The comma-separated list of hostname and port pairs of the MongoDB servers. "
                                    + "eg. localhost:27017,localhost:27018");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the database user to be used when connecting to MongoDB. "
                                    + "This is required only when MongoDB is configured to use authentication.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to be used when connecting to MongoDB. "
                                    + "This is required only when MongoDB is configured to use authentication.");

    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the database to watch for changes.");

    public static final ConfigOption<String> COLLECTION =
            ConfigOptions.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the collection in the database to watch for changes.");

    public static final ConfigOption<String> CONNECTION_OPTIONS =
            ConfigOptions.key("connection.options")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The ampersand-separated MongoDB connection options. "
                                    + "eg. replicaSet=test&connectTimeoutMS=300000");

    public static final ConfigOption<Integer> COPY_EXISTING_QUEUE_SIZE =
            ConfigOptions.key("copy.existing.queue.size")
                    .intType()
                    .defaultValue(10240)
                    .withDescription(
                            "The max size of the queue to use when copying data. Defaults to 10240.");

    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("The cursor batch size. Defaults to 1024.");

    public static final ConfigOption<Integer> POLL_MAX_BATCH_SIZE =
            ConfigOptions.key("poll.max.batch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "Maximum number of change stream documents "
                                    + "to include in a single batch when polling for new data. "
                                    + "This setting can be used to limit the amount of data buffered internally in the connector. "
                                    + "Defaults to 1024.");

    public static final ConfigOption<Integer> POLL_AWAIT_TIME_MILLIS =
            ConfigOptions.key("poll.await.time.ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The amount of time to wait before checking for new results on the change stream."
                                    + "Defaults: 1000.");

    public static final ConfigOption<Integer> HEARTBEAT_INTERVAL_MILLIS =
            ConfigOptions.key("heartbeat.interval.ms")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The length of time in milliseconds between sending heartbeat messages."
                                    + "Heartbeat messages contain the post batch resume token and are sent when no source records "
                                    + "have been published in the specified interval. This improves the resumability of the connector "
                                    + "for low volume namespaces. Use 0 to disable. Defaults to 0.");

    @Experimental
    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_SNAPSHOT_ENABLED =
            ConfigOptions.key("scan.incremental.snapshot.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether enable incremental snapshot. Defaults to false.");

    @Experimental
    public static final ConfigOption<Integer> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB =
            ConfigOptions.key("scan.incremental.snapshot.chunk.size.mb")
                    .intType()
                    .defaultValue(64)
                    .withDescription(
                            "The chunk size mb of incremental snapshot. Defaults to 64mb.");

    public static final ConfigOption<DebeziumChangelogMode> CHANGELOG_MODE =
            ConfigOptions.key("changelog-mode")
                    .enumType(DebeziumChangelogMode.class)
                    .defaultValue(DebeziumChangelogMode.ALL)
                    .withDescription(
                            "The changelog mode used for encoding streaming changes.\n"
                                    + "\"all\": Encodes changes as retract stream using all RowKinds. This is the default mode.\n"
                                    + "\"upsert\": Encodes changes as upsert stream that describes idempotent updates on a key. It can be used for tables with primary keys when replica identity FULL is not an option.");
}
