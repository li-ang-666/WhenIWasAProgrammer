package com.liang.spark.job;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.service.database.holder.DruidHolder;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.DateTimeUtils;
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
        SparkSession spark = SparkSessionFactory.createSpark(args);
        String fromTable = args[1];
        log.info("fromTable: {}", fromTable);
        String toTable = args[2];
        log.info("toTable: {}", toTable);
        Tuple2<Long, Long> minAndMax = new JdbcTemplate("source").queryForObject(
                String.format("select min(id),max(id) from %s", fromTable),
                rs -> Tuple2.of(rs.getLong(1), rs.getLong(2)));
        long minId = minAndMax.f0;
        log.info("minId: {}", minId);
        long maxId = minAndMax.f1;
        log.info("maxId: {}", maxId);
        DruidDataSource dataSource = new DruidHolder().getPool("source");
        String url = dataSource.getUrl();
        log.info("jdbc url: {}", url);
        String user = dataSource.getUsername();
        log.info("jdbc user: {}", user);
        String password = dataSource.getPassword();
        log.info("jdbc password: {}", password);
        spark.read().option("fetchsize", "10240").jdbc(
                url,
                fromTable,
                "id",
                minId,
                maxId,
                (int) ((maxId - minId) / 500000),
                new Properties() {{
                    put("user", user);
                    put("password", password);
                }}
        ).createTempView("source");

        spark.sql(
                String.format("insert overwrite table %s(pt = '%s') select /*+ REPARTITION(10) */ * from source",
                        toTable, DateTimeUtils.fromUnixTime(System.currentTimeMillis() / 1000, "yyyyMMdd"))
        );
        spark.stop();
    }
}
