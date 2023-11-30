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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.io.IOException;
import java.util.Optional;

@AutoService(SeaTunnelSink.class)
public class IcebergSink extends AbstractSink<SeaTunnelRow, Void> {

    @Override
    public String getPluginName() {
        return "Iceberg";
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) throws IOException {
        return new AbstractSinkWriter<SeaTunnelRow, Void>() {
            IcebergSinkWrite icebergSinkWrite = new IcebergSinkWrite(pluginConfig, seaTunnelRowType);
            @Override
            public void write(SeaTunnelRow element) throws IOException {
                icebergSinkWrite.write((SeaTunnelRow) element);
            }

            @Override
            public Optional<Void> prepareCommit() {
                return icebergSinkWrite.prepareCommit();
            }

            @Override
            public void close() throws IOException {
                icebergSinkWrite.close();
            }
        };
    }
}
