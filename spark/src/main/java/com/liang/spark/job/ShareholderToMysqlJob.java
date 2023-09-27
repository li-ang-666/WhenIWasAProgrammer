package com.liang.spark.job;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.dto.Config;
import com.liang.common.service.database.factory.DruidFactory;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ShareholderToMysqlJob {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        DruidDataSource pool = new DruidFactory().createPool("427.test");
        HashMap<String, String> configMap = new HashMap<String, String>() {{
            put("driver", pool.getDriverClassName());
            put("url", pool.getUrl());
            put("dbtable", "test.ratio_path_company");
            put("user", pool.getUsername());
            put("password", pool.getPassword());
            put("batchsize", "1024");
            put("truncate", "true");
        }};
        TableFactory.jdbc(spark, "457.prism_shareholder_path", "ratio_path_company")
                .write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .options(configMap).save();
    }

    private final static class Sink implements ForeachPartitionFunction<Row> {
        private final Config config;
        private final String sink;

        public Sink(Config config, String sink) {
            this.config = config;
            this.sink = sink;
        }

        @Override
        public void call(Iterator<Row> t) throws Exception {
            ConfigUtils.setConfig(config);
            JdbcTemplate jdbcTemplate = new JdbcTemplate("sink");
            jdbcTemplate.enableCache(1000 * 10, 1024);
            while (t.hasNext()) {
                Row row = t.next();
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(row.json());
                columnMap.remove("id");
                columnMap.remove("pt");
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
                String sql = String.format("insert ignore into %s(%s)values(%s)", sink, insert.f0, insert.f1);
                jdbcTemplate.update(sql);
            }
            jdbcTemplate.flush();
        }
    }
}
