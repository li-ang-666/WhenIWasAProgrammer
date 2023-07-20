package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;
import java.util.Map;

@Slf4j
public class V1ToV2Job {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        TableFactory.jdbc(spark, "bdpEquity", "entity_controller_details")
                .createOrReplaceTempView("t");
        spark.sql("select * from t")
                .repartition(1200)
                .foreachPartition(new V1ToV2Sink(ConfigUtils.getConfig(), "bdpEquity", "entity_controller_details_v2"));
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class V1ToV2Sink implements ForeachPartitionFunction<Row> {
        private final Config config;
        private final String sourceName;
        private final String tableName;

        @Override
        public void call(Iterator<Row> iterator) throws Exception {
            ConfigUtils.setConfig(config);
            JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceName);
            jdbcTemplate.enableCache(5000, 1024);
            while (iterator.hasNext()) {
                Row row = iterator.next();
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(row.json());
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
                String sql = new SQL()
                        .REPLACE_INTO(tableName)
                        .INTO_COLUMNS(insert.f0)
                        .INTO_VALUES(insert.f1)
                        .toString();
                jdbcTemplate.update(sql);
            }
            jdbcTemplate.flush();
        }
    }
}
