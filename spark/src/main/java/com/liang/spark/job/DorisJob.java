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
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;

public class DorisJob {
    private static final String REGEX = "insert +into +([a-z1-9_]+)\\.([a-z1-9_]+) +(select.*)";

    public static void main(String[] args) {
        String sql = args[0];
        String dorisDatabase = sql.replaceAll(REGEX, "$1");
        String dorisTable = sql.replaceAll(REGEX, "$2");
        String sparkSql = sql.replaceAll(REGEX, "$3");
        SparkSession spark = SparkSessionFactory.createSpark(null);
        spark.sql(sparkSql)
                .repartition(2)
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
            DorisSchema schema = DorisSchema.builder()
                    .database(database)
                    .tableName(table)
                    .uniqueDeleteOn(DorisSchema.DEFAULT_UNIQUE_DELETE_ON)
                    .build();
            DorisTemplate dorisSink = new DorisTemplate("dorisSink");
            dorisSink.enableCache();
            while (iterator.hasNext()) {
                DorisOneRow dorisOneRow = new DorisOneRow(schema, JsonUtils.parseJsonObj(iterator.next().json()));
                dorisOneRow.put("__DORIS_DELETE_SIGN__", 0);
                dorisSink.update(dorisOneRow);
            }
            dorisSink.flush();
        }
    }
}
