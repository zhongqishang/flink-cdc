package com.qichacha.cdc.connectors.iceberg.sink;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

import java.util.Map;

public class CatalogPropertiesUtils {

    public static Map<String, String> getProperties(String database) {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put("catalog-database", database);
        properties.put("uri", "thrift://localhost:9083");
        properties.put("hive-conf-dir", "/opt/hadoop-2.10.2/etc/hadoop");
        properties.put("write-format", "parquet");
        properties.put("write.upsert.enabled", "true");
        properties.put("overwrite-enabled", "false");
        properties.put("engine.hive.enabled", "true");
        return properties.build();
    }
}
