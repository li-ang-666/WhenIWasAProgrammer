package com.liang.spark.job;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.holder.DruidHolder;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class V1ToV2Job {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        String fromTemplate = "bdpEquity";
        String fromTable = "entity_controller_details";
        String toTemplate = "bdpEquity";
        String toTable = "entity_controller_details_v2";
        Tuple2<Long, Long> minAndMax = new JdbcTemplate(fromTemplate).queryForObject(
                String.format("select min(id),max(id) from %s", fromTable),
                rs -> Tuple2.of(rs.getLong(1), rs.getLong(2)));
        long minId = minAndMax.f0;
        long maxId = minAndMax.f1;
        DruidDataSource dataSource = new DruidHolder().getPool(fromTemplate);
        String url = dataSource.getUrl();
        String user = dataSource.getUsername();
        String password = dataSource.getPassword();
        spark.read().option("fetchsize", "2048").jdbc(
                        url,
                        fromTable,
                        "id",
                        minId,
                        maxId,
                        (int) ((maxId - minId) / 10240),
                        new Properties() {{
                            put("user", user);
                            put("password", password);
                        }}
                )
                .createTempView("t");
        spark.sql("select * from t")
                .repartition(1200)
                .foreachPartition(new V1ToV2Sink(ConfigUtils.getConfig(), toTemplate, toTable));
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class V1ToV2Sink implements ForeachPartitionFunction<Row> {
        private final Config config;
        private final String toTemplate;
        private final String toTable;

        @Override
        public void call(Iterator<Row> iterator) throws Exception {
            ConfigUtils.setConfig(config);
            JdbcTemplate jdbcTemplate = new JdbcTemplate(toTemplate);
            jdbcTemplate.enableCache(5000, 1024);
            while (iterator.hasNext()) {
                Row row = iterator.next();
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(row.json());
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
                String sql = new SQL()
                        .REPLACE_INTO(toTable)
                        .INTO_COLUMNS(insert.f0)
                        .INTO_VALUES(insert.f1)
                        .toString();
                jdbcTemplate.update(sql);
            }
            jdbcTemplate.flush();
        }
    }
}
