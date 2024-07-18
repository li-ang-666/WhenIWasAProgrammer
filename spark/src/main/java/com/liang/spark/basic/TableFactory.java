package com.liang.spark.basic;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.service.database.holder.DruidHolder;
import com.liang.common.service.database.template.JdbcTemplate;
import lombok.experimental.UtilityClass;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

@UtilityClass
public class TableFactory {
    public static Dataset<Row> csv(SparkSession spark, String fileName) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("file:/Users/liang/Desktop/WhenIWasAProgrammer/spark/src/main/resources/" + fileName);
    }

    public static Dataset<Row> jdbc(SparkSession spark, String sourceName, String tableName) {
        Tuple2<Long, Long> minAndMax = new JdbcTemplate(sourceName).queryForObject(
                String.format("select min(id),max(id) from %s", tableName),
                rs -> Tuple2.of(rs.getLong(1), rs.getLong(2)));
        long minId = minAndMax.f0;
        long maxId = minAndMax.f1;
        DruidDataSource dataSource = new DruidHolder().getPool(sourceName);
        String url = dataSource.getUrl();
        String user = dataSource.getUsername();
        String password = dataSource.getPassword();
        return spark.read().option("fetchsize", String.valueOf(Integer.MIN_VALUE)).jdbc(
                url,
                tableName,
                "id",
                minId,
                maxId,
                1,
                new Properties() {{
                    put("user", user);
                    put("password", password);
                }}
        );
    }
}
