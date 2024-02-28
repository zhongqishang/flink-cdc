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

package com.ververica.cdc.connectors.base.source.meta.events;

import org.apache.flink.api.connector.source.SourceEvent;

/**
 * The {@link SourceEvent} that {@link
 * com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReader} sends to {@link
 * com.ververica.cdc.connectors.base.source.enumerator.IncrementalSourceEnumerator} to ask the
 * latest finished snapshot splits number.
 */
public class LatestFinishedSplitsNumberRequestEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    public LatestFinishedSplitsNumberRequestEvent() {}
}