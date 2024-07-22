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

package com.qichacha.cdc.connectors.iceberg.sink;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

import java.util.Map;

/** CatalogPropertiesUtils. */
public class CatalogPropertiesUtils {

    public static Map<String, String> getProperties(String database) {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put("catalog-database", database);
        properties.put(
                "uri", "thrift://hd-021.ld-hadoop.com:9083,thrift://hd-022.ld-hadoop.com:9083");
        properties.put("write-format", "parquet");
        properties.put("write.upsert.enabled", "true");
        properties.put("overwrite-enabled", "false");
        properties.put("engine.hive.enabled", "true");
        properties.put("write.metadata.metrics.default", "full");
        properties.put("write.target-file-size-bytes", "268435456");
        properties.put("write.parquet.compression-codec", "zstd");
        properties.put("self-optimizing.group", "flinkGroup");
        return properties.build();
    }
}
