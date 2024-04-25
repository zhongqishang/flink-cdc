package com.qichacha.cdc.connectors.iceberg.sink;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;

/** Partitioner that send {@link PartitioningEvent} to its target partition. */
public class PartitioningEventPartitioner implements Partitioner<Integer> {
    @Override
    public int partition(Integer target, int numPartitions) {
        if (target >= numPartitions) {
            return target % numPartitions;
        }
        return target;
    }
}
