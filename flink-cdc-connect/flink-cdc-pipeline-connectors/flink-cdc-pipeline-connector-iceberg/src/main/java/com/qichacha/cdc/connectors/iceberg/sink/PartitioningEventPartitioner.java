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

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;

/** Partitioner that send {@link PartitioningEvent} to its target partition. */
public class PartitioningEventPartitioner implements Partitioner<Integer> {
    @Override
    public int partition(Integer target, int numPartitions) {
        if (target < 0) {
            throw new IllegalArgumentException("Target should be greater than or equal to 0");
        } else if (target >= numPartitions) {
            return target % numPartitions;
        }
        return target;
    }
}
