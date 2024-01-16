package com.liang.spark.job;

import com.liang.common.dto.Config;
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

import java.util.Iterator;
import java.util.Map;

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
        private final Config config;
        private final String database;
        private final String table;

        @Override
        public void call(Iterator<Row> iterator) {
            ConfigUtils.setConfig(config);
            DorisSchema schema = DorisSchema.builder().database(database).tableName(table).build();
            DorisTemplate dorisSink = new DorisTemplate("dorisSink", schema);
            while (iterator.hasNext()) {
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(iterator.next().json());
                dorisSink.updateBatch(columnMap);
            }
            dorisSink.flushBatch();
        }
    }
}
