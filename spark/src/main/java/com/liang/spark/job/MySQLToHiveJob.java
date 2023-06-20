package com.liang.spark.job;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.service.database.holder.DruidHolder;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

@Slf4j
public class MySQLToHiveJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{"mysql-to-hive.yml"};
        }
        String fromTable = args[1];
        String toTable = args[2];
        Tuple2<Long, Long> minAndMax = new JdbcTemplate("source").queryForObject(
                String.format("select min(id),max(id) from %s", fromTable),
                rs -> Tuple2.of(rs.getLong(1), rs.getLong(2)));
        DruidDataSource dataSource = new DruidHolder().getPool("source");
        SparkSession spark = SparkSessionFactory.createSpark(args);
        spark.read().option("fetchsize", "2048").jdbc(
                dataSource.getUrl(),
                fromTable,
                "id",
                minAndMax.f0,
                minAndMax.f1,
                (int) ((minAndMax.f1 - minAndMax.f0) / 500000),
                new Properties() {{
                    put("user", dataSource.getUrl());
                    put("password", dataSource.getPassword());
                }}
        ).createTempView("source");

        spark.sql(
                String.format("insert overwrite table %s select /*+ REPARTITION(10) */ * from sourceTable", toTable)
        );
        spark.stop();
    }
}
