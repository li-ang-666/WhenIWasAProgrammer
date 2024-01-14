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

@Slf4j
public class DorisCopyJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(null);
        spark.read().format("doris")
                .option("doris.fenodes", "10.99.197.34:8030,10.99.202.71:8030,10.99.203.88:8030")
                .option("doris.table.identifier", args[0] + "." + args[1])
                .option("user", "dba")
                .option("password", "Tyc@1234")
                .option("doris.batch.size", "102400")
                .load()
                .foreachPartition(new DorisSink(ConfigUtils.getConfig(), args[2], args[3]));
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
