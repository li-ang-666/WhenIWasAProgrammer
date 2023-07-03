package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.spark.basic.SparkSessionFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;
import java.util.Map;

public class ShareholderToMysqlJob {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        String arg = args[1];
        String source;
        String sink;
        if ("1".equals(arg)) {
            source = "dwc.dwc_new_tab3_entity_beneficiary_details_df";
            sink = "entity_beneficiary_details";
        } else if ("2".equals(arg)) {
            source = "dwc.dwc_new_tab3_entity_controller_details_df";
            sink = "entity_controller_details";
        } else {
            throw new RuntimeException();
        }

        spark.sql(String.format("select * from %s where pt = '20230702' ", source))
                .repartition(1200)
                .foreachPartition(new Sink(ConfigUtils.getConfig(), sink));
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
