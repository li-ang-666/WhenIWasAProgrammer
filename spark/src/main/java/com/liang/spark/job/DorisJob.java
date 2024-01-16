package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class DorisJob {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        // sql
        String sparkSql = parameterTool.get("sparkSql");
        log.info("sparkSql: {}", sparkSql);
        // db
        String sinkDatabase = parameterTool.get("sinkDatabase");
        log.info("sinkDatabase: {}", sinkDatabase);
        // tb
        String sinkTable = parameterTool.get("sinkTable");
        log.info("sinkTable: {}", sinkTable);
        // exec
        SparkSession spark = SparkSessionFactory.createSpark(null);
        spark.sql(sparkSql)
                .repartition()
                .foreachPartition(new DorisSink(ConfigUtils.getConfig(), sinkDatabase, sinkTable));
    }

    @Slf4j
    @RequiredArgsConstructor
    public final static class DorisSink implements ForeachPartitionFunction<Row> {
        private static final int BATCH_SIZE = 4 * 1024 * 1024;
        private final List<DorisOneRow> rows = new ArrayList<>(BATCH_SIZE);
        private final Config config;
        private final String database;
        private final String table;

        @Override
        public void call(Iterator<Row> iterator) {
            ConfigUtils.setConfig(config);
            DorisTemplate dorisSink = new DorisTemplate("dorisSink");
            DorisSchema schema = DorisSchema.builder()
                    .database(database)
                    .tableName(table)
                    .build();
            while (iterator.hasNext()) {
                rows.add(new DorisOneRow(schema, JsonUtils.parseJsonObj(iterator.next().json())));
                if (rows.size() >= BATCH_SIZE) {
                    dorisSink.update(rows);
                    rows.clear();
                }
            }
            if (!rows.isEmpty()) {
                dorisSink.update(rows);
                rows.clear();
            }
        }
    }
}
