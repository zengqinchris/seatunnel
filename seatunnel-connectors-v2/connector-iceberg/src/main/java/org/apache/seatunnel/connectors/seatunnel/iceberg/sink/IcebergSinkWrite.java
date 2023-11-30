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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.ThreadPools;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergCatalogFactoryV2;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorException;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iceberg.expressions.Expressions.*;

@Slf4j
public class IcebergSinkWrite extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private SeaTunnelRowType seaTunnelRowType;
    private Config icebergConfig;
    private Catalog catalog;
    private Table table;
    private Schema tableSchema;
    private GenericRecord record;
    private GenericRecord copy;
    private FileAppenderFactory appenderFactory;
    private TableIdentifier tableIdentifier;
    private IcebergCatalogFactoryV2 icebergCatalogFactory;
    private List<Object[]> batchList = new ArrayList<>();
    private AtomicInteger counter = new AtomicInteger(0);
    private Configuration config = new Configuration();
    private volatile boolean initialize = false;
    private Map<String, Boolean> initCache = new HashMap<>();
    private Map<String, PartitionSpec> cacheSpec = new HashMap<>();
    private Map<String, List<Record>> cacheData = new HashMap<>();

    public IcebergSinkWrite(Config icebergConfig, SeaTunnelRowType seaTunnelRowType) {
        this.icebergConfig = icebergConfig;
        this.seaTunnelRowType = seaTunnelRowType;
    }

    private void tryInit() {
        if (initialize) {
            return;
        }
        config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        config.addResource(new Path(icebergConfig.getString("core_site_path")));
        config.addResource(new Path(icebergConfig.getString("hdfs_site_path")));
        icebergCatalogFactory = new IcebergCatalogFactoryV2(icebergConfig.getString("catalog_name"),
                IcebergCatalogType.valueOf(icebergConfig.getString("catalog_type").toUpperCase()),
                icebergConfig.getString("warehouse"), icebergConfig.getString("uri"), config);
        catalog = CachingCatalog.wrap(icebergCatalogFactory.create());
        tableIdentifier = TableIdentifier.of(icebergConfig.getString("namespace"), icebergConfig.getString("table_name"));
        table = catalog.loadTable(tableIdentifier);
        tableSchema = table.schema();
        record = GenericRecord.create(tableSchema);
        appenderFactory = new GenericAppenderFactory(tableSchema);
        initialize = true;
    }

    @Override
    public void write(SeaTunnelRow element) {
        tryInit();
        batchList.add(element.getFields());
        if (counter.getAndIncrement() >= icebergConfig.getInt("batch_size")) {
            flush();
            counter.set(0);
        }
    }

    private void flush() {
        if (batchList.isEmpty()) {
            return;
        }
        try {
            String warehouse = icebergConfig.getString("warehouse");
            String tableName = icebergConfig.getString("table_name");
            String namespace = icebergConfig.getString("namespace");
            String partitionIdx = icebergConfig.getString("partition_index");
            String sourceFlag = icebergConfig.getString("source_flag");
            String orderColumn = icebergConfig.getString("order_column");
            String[] orderSplit = orderColumn.replace(", ", ",").split(",");
            ConfigObject fields = icebergConfig.getObject("fields");
            Map<String, Object> unwrapped = fields.unwrapped();
            String partition = icebergConfig.getString("partition");
            List<Record> records = new ArrayList<>();
            String partitionPath = "";
            Integer partitionIndex = Integer.valueOf(partitionIdx);
            String regex = "\\d{4}-\\d{2}-\\d{2}(.*)";

            for (Object[] fieldValue : batchList) {
                Object typeValue = "";
                String type = "";
                String value = "";
                copy = record.copy();
                int k = 0;

                if (!"".equals(partition)) {
                    String partValue = fieldValue[partitionIndex].toString().replace("\"", "");
                    String partVal = "";
                    if (partValue.contains("T")) {
                        partVal = partValue.split("\\+")[0].replace("T", " ");
                    } else {
                        partVal = partValue;
                    }
                    if (partVal.matches(regex)) {
                        partitionPath = partition + "=" + partVal.substring(0, 10);
                    } else {
                        partitionPath = partition + "=" + fieldValue[partitionIndex].toString();
                    }
//                System.out.println("=============first===partition====================" + partition);
                    if (cacheSpec.getOrDefault(partitionPath, null) == null) {
                        PartitionSpec spec = PartitionSpec.builderFor(tableSchema).identity(partition).build();
                        cacheSpec.put(partitionPath, spec);
                        List<Record> recordDT = new ArrayList<>();
                        cacheData.put(partitionPath, recordDT);
                        initCache.put(partitionPath, false);
                    }
                }
                List<Record> recordSpec = cacheData.get(partitionPath);
                for (int i = 0; i < orderSplit.length; i++) {
                    value = "";
                    k = i;
                    if (k >= partitionIndex && !"".equals(partition) && !"Iceberg".equals(sourceFlag)) {
                        k = k + 1;
                    }
                    type = unwrapped.get(orderSplit[i].trim()).toString().toLowerCase();
                    if (!"".equals(fieldValue[k]) && !" ".equals(fieldValue[k])) {
                        value = fieldValue[k] != null ? String.valueOf(fieldValue[k]) : "";
                    }
                    if ("integer".equals(type) || "int".equals(type) || "smallint".equals(type) || "tinyint".equals(type) || "short".equals(type) || "byte".equals(type)) {
//                    System.out.println("=========================long================================" + value);
                        typeValue = NumberUtils.toInt(value);
                    } else if ("boolean".equals(type)) {
//                    System.out.println("=========================boolean================================" + value);
                        typeValue = !"".equals(value) ? Boolean.valueOf(value) : null;
                    } else if ("binary".equals(type)) {
//                    System.out.println("=========================binary================================" + value);
                        typeValue = !"".equals(value) ? ByteBuffer.wrap(value.getBytes()) : null;
                    } else if ("decimal".equals(type) || type.contains("decimal")) {
//                    System.out.println("=========================decimal================================" + value);
//                    System.out.println("=================decimal========type================================" + type);
//                        2.11
                        String[] splitVal = new String[2];
                        String newVal = "0";
                        if (value.contains(".")) {
                            splitVal = value.split("\\.");
//                        System.out.println("=================split[0]========" + splitVal[0] + "============split[1]====================" + splitVal[1]);
                            newVal = splitVal[1];
                        } else {
                            splitVal[0] = value;
                            splitVal[1] = "0";
                        }
//                        decimal(10.5), decimal(10.0)
                        String lenStr = type.replace("decimal(", "").replace(")", "").split("\\.")[1];
                        Integer len = Integer.valueOf(lenStr);
                        int length = newVal.length();
//                    System.out.println("=================len========" + len + "================length================" + length);
                        if (len > length) {
                            for (int mm = 0; mm < (len - length); mm++) {
                                newVal += "0";
                            }
                            value = splitVal[0] + "." + newVal;
                        }
                        typeValue = NumberUtils.createBigDecimal(value);
                    } else if ("time".equals(type)) {
//                    System.out.println("=========================time================================" + value);
                        typeValue = !"".equals(value) ? LocalTime.parse(value.replace("\"", "")) : null;
                    } else if ("date".equals(type)) {
//                    System.out.println("=========================date================================" + value);
                        if (!"".equals(value)) {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                            Date date = null;
                            try {
                                date = sdf.parse(value.replace("\"", ""));
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            String value2 = sdf.format(date);
                            typeValue = LocalDate.parse(value2);
                        } else {
                            typeValue = null;
                        }
                    } else if ("timestamp".equals(type) || "datetime".equals(type)) {
//                    System.out.println("=========================timestamp================================" + value);
                        if (!"".equals(value)) {
                            String strV = value.replace("\"", "").replace("T", " ");
                            if (strV.length() == 16) {
                                strV = strV + ":00";
                            } else if (strV.length() == 13) {
                                strV = strV + ":00:00";
                            } else if (strV.length() == 10) {
                                strV = strV + " 00:00:00";
                            }
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            Date date = null;
                            try {
                                date = sdf.parse(strV);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            String value2 = sdf.format(date);
                            typeValue = OffsetDateTime.parse(value2.replace(" ", "T") + "+08:00");
                        } else {
                            typeValue = null;
                        }

                    } else if ("long".equals(type) || "bigint".equals(type)) {
                        typeValue = NumberUtils.toLong(value);
                    } else if ("float".equals(type)) {
//                    typeValue = !"".equals(value) ? Float.parseFloat(value + "f") : 0.0f;
                        typeValue = NumberUtils.toFloat(value + "f");
                    } else if ("double".equals(type)) {
                        typeValue = NumberUtils.toDouble(value + "d");
                    } else {
//                    System.out.println("=========================else================================" + value);
                        typeValue = value;
                    }
                    copy.setField(orderSplit[i].trim(), typeValue);
                }
                copy.setField("sink_time", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                records.add(copy);
                if (!"".equals(partition)) {
                    recordSpec.add(copy);
                    cacheData.put(partitionPath, recordSpec);
                }
            }

            if ("".equals(partition)) {
                partitionPath = tableName;
                initCache.put(partitionPath, false);
                writeDataToIceber(warehouse, tableName, namespace, partition, records, partitionPath, null, batchList.size());
            } else {
                for (Map.Entry<String, List<Record>> entry : cacheData.entrySet()) {
                    writeDataToIceber(warehouse, tableName, namespace, partition, entry.getValue(), entry.getKey(), cacheSpec.get(entry.getKey()), batchList.size());
                }
            }
            if (!"".equals(partition)) {
                cacheData.get(partitionPath).clear();
            }
            batchList.clear();

            // Log the Metaspace memory usage
            MemoryPoolMXBean metaspacePool = ManagementFactory.getMemoryPoolMXBeans()
                    .stream()
                    .filter(pool -> pool.getName().contains("Metaspace"))
                    .findFirst()
                    .orElse(null);

            if (metaspacePool != null) {
                System.out.println("flush ---> Metaspace usage: " +
                        "Used = " + metaspacePool.getUsage().getUsed() +
                        ", Max = " + metaspacePool.getUsage().getMax());
            } else {
                System.out.println("flush ---> Metaspace memory pool not found.");
            }

        } catch (Exception e) {
            throw new RuntimeException("写入数据到Iceberg出错=>" + e);
        }
    }

    private void writeDataToIceber(String warehouse, String tableName, String namespace, String partition, List<Record> records,
                                   String partitionPath, PartitionSpec spec, int batchSize) throws Exception {
//        System.out.println("=========================partitionPath================================" + partitionPath);
        long currentTimeMillis = System.currentTimeMillis();
        String saveMode = icebergConfig.getString("save_mode");
        if (!"default".equals(namespace)) {
            tableName = namespace + ".db/" + tableName;
        }
        String externalFilePath = "".equals(partitionPath) ?
                String.format(warehouse + "/" + tableName + "/data/" + currentTimeMillis + "_%s.avro", counter.get()) :
                String.format(warehouse + "/" + tableName + "/data/" + partitionPath + "/" + currentTimeMillis + "_%s.avro", counter.get());
        Path path = new Path(externalFilePath);
        OutputFile outputFile = HadoopOutputFile.fromPath(path, config);
        FileAppender<Record> fileAppender = appenderFactory.newAppender(outputFile, FileFormat.fromFileName(externalFilePath));
        try (FileAppender<Record> fileAppenderCloseable = fileAppender) {
            fileAppenderCloseable.addAll(records);
            records.clear();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        DataFile datafile = null;
        try {
            if ("".equals(partition)) {
                datafile = DataFiles.builder(PartitionSpec.unpartitioned())
                        .withInputFile(HadoopInputFile.fromPath(path, config))
                        .withFileSizeInBytes(fileAppender.length())
                        .withRecordCount(batchSize)
                        .withMetrics(fileAppender.metrics())
                        .build();
            } else {
                datafile = DataFiles.builder(spec)
                        .withInputFile(HadoopInputFile.fromPath(path, config))
                        .withPath(externalFilePath)
                        .withFileSizeInBytes(fileAppender.length())
                        .withRecordCount(batchSize)
                        .withPartitionPath(partitionPath)
                        .withMetrics(fileAppender.metrics())
                        .build();
            }

            if ("overwrite".equals(saveMode)) {
    //                table.newRewrite();
    //                table.rewriteManifests();
    //                table.newReplacePartitions();
    //                table.newOverwrite().overwriteByRowFilter(Expressions.greaterThan("id", 4)).commit();
    //                commit(table, table.newOverwrite().overwriteByRowFilter(equal("date", "2018-06-08")), branch);

                if (partitionPath.contains("=")) {
                    String[] keyValue = partitionPath.split("=");
                    if (!initCache.get(partitionPath)) {
                        table.newOverwrite().overwriteByRowFilter(equal(keyValue[0], keyValue[1])).deleteFile(datafile).addFile(datafile).commit();
                        initCache.put(partitionPath, true);
                    } else {
                        table.newOverwrite().addFile(datafile).commit();
                    }
                } else {
                    if (!initCache.get(partitionPath)) {
                        table.newOverwrite().overwriteByRowFilter(alwaysTrue()).deleteFile(datafile).addFile(datafile).commit();
                        initCache.put(partitionPath, true);
                    } else {
                        table.newOverwrite().addFile(datafile).commit();
                    }
    //                table.newOverwrite().overwriteByRowFilter(notEqual("","")).addFile(datafile).commit();
    //                throw new RuntimeException("覆盖写入必须要分区字段和分区值!!!");
                }
            } else {
                table.newAppend().appendFile(datafile).commit();
            }

            Snapshot snapshot = table.currentSnapshot();
            long expireTime = System.currentTimeMillis() - 86400 * 1000;
            if (snapshot != null) {
                expireTime = snapshot.timestampMillis();
            }
            table.expireSnapshots().expireOlderThan(expireTime).commit();
            table.updateProperties().set("write.metadata.delete-after-commit.enabled", "true")
                    .set("write.metadata.previous-versions-max", "2").commit();

        } catch (Exception e) {
            log.error("写入iceberg表数据异常=>" + e);
        } finally {
            fileAppender.close();
        }

    }

    @Override
    public List<Void> snapshotState(long checkpointId) throws IOException {
        return super.snapshotState(checkpointId);
    }

    @Override
    public Optional<Void> prepareCommit() {
        try {
            flush();
        } catch (Exception e) {
            throw new IcebergConnectorException(IcebergConnectorErrorCode.TRANSACTION_OPERATION_FAILED, "commit failed," + e.getMessage(), e);
        }
        return super.prepareCommit();
    }

    @Override
    public void close() throws IOException {
        flush();

        // Log the Metaspace memory usage
        MemoryPoolMXBean metaspacePool = ManagementFactory.getMemoryPoolMXBeans()
                .stream()
                .filter(pool -> pool.getName().contains("Metaspace"))
                .findFirst()
                .orElse(null);

        if (metaspacePool != null) {
            System.out.println("close ---> Metaspace usage: " +
                    "Used = " + metaspacePool.getUsage().getUsed() +
                    ", Max = " + metaspacePool.getUsage().getMax());
        } else {
            System.out.println("close ---> Metaspace memory pool not found.");
        }


        if (table != null) {
            table.io().close();
        }
        if (batchList.size()>0) {
            batchList.clear();
        }
        if (initCache.size()>0) {
            initCache.clear();
        }
        if (cacheSpec.size()>0) {
            cacheSpec.clear();
        }
        if (cacheData.size()>0) {
            cacheData.clear();
        }
        if (catalog != null && catalog instanceof Closeable) {
            ((Closeable) catalog).close();
        }

        ExecutorService workerPool = ThreadPools.getWorkerPool();
        workerPool.shutdown();
//        try {
//            Class<?> threadPoolsClass = Class.forName("org.apache.iceberg.util.ThreadPools");
//            Method getThreadPoolMethod = threadPoolsClass.getDeclaredMethod("getWorkerPool");
//            ExecutorService genericPool = (ExecutorService) getThreadPoolMethod.invoke(null);
//            genericPool.shutdown();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
