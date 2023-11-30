/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.common.sink;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractSink<T, StateT> implements SeaTunnelSink<T, StateT, Void, Void> {
    protected SeaTunnelRowType seaTunnelRowType;
    protected Config pluginConfig;

    @Override
    public void prepare(Config pluginConfig) {
        this.pluginConfig = pluginConfig;
    }

    @Override
    public final Optional<SinkCommitter<Void>> createCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public final Optional<Serializer<Void>> getCommitInfoSerializer() {
        return Optional.empty();
    }

    @Override
    public final Optional<SinkAggregatedCommitter<Void, Void>> createAggregatedCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public final Optional<Serializer<Void>> getAggregatedCommitInfoSerializer() {
        return Optional.empty();
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public Map<String, String> getPluginConfig() {
        return TypesafeConfigUtils.extractPluginConfig(this.pluginConfig);
    }
}
