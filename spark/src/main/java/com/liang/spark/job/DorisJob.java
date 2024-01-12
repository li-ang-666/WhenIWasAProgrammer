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

import java.util.Iterator;

@Slf4j
public class DorisJob {
    private static final String REGEX = "insert\\s+into\\s+(\\w+)\\.(\\w+)\\s+(select.*)";

    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String sql = parameterTool.get("sql");
        int parallelism = parameterTool.getInt("parallelism");
        log.info("sql: {}, parallelism: {}", sql, parallelism);
        String dorisDatabase = sql.replaceAll(REGEX, "$1");
        String dorisTable = sql.replaceAll(REGEX, "$2");
        String sparkSql = sql.replaceAll(REGEX, "$3");
        log.info("dorisDatabase: {}, dorisTable: {}, sparkQuerySql: {}", dorisDatabase, dorisTable, sparkSql);
        SparkSession spark = SparkSessionFactory.createSpark(null);
        spark.sql(sparkSql)
                .repartition(parallelism)
                .foreachPartition(new DorisSink(ConfigUtils.getConfig(), dorisDatabase, dorisTable));
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class DorisSink implements ForeachPartitionFunction<Row> {
        private final Config config;
        private final String database;
        private final String table;

        @Override
        public void call(Iterator<Row> iterator) {
            ConfigUtils.setConfig(config);
            DorisTemplate dorisSink = new DorisTemplate("dorisSink");
            dorisSink.enableCache();
            DorisSchema schema = DorisSchema.builder()
                    .database(database)
                    .tableName(table)
                    .build();
            while (iterator.hasNext()) {
                dorisSink.update(new DorisOneRow(schema, JsonUtils.parseJsonObj(iterator.next().json())));
            }
            dorisSink.flush();
        }
    }
}
