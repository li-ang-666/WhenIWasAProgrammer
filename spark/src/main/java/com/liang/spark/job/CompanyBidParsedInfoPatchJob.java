package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CompanyBidParsedInfoPatchJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        Config config = ConfigUtils.getConfig();
        String sql = new SQL()
                .SELECT("bid_uuid uuid")
                .SELECT("get_json_object(`json`,$.result.entities) entities")
                .FROM("test.algorithm_origin_bid_html_parsed_entity_v1")
                .WHERE("get_json_object(`json`,$.result.entities) is not null")
                .WHERE("uuid is not null and uuid <> ''")
                .toString();
        spark.sql(sql)
                .orderBy(new Column("uuid"))
                .foreachPartition(new CompanyBidParsedInfoPatchSink(config));
    }

    @RequiredArgsConstructor
    private final static class CompanyBidParsedInfoPatchSink implements ForeachPartitionFunction<Row> {
        private final static int BATCH_SIZE = 1024;
        private final Config config;

        @Override
        public void call(Iterator<Row> iterator) {
            ConfigUtils.setConfig(config);
            JdbcTemplate gauss = new JdbcTemplate("gauss");
            List<Map<String, Object>> columnMaps = new ArrayList<>(BATCH_SIZE);
            while (iterator.hasNext()) {
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(iterator.next().json());
                columnMaps.add(columnMap);
                if (columnMaps.size() >= BATCH_SIZE) {
                    Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMaps);
                    gauss.update(String.format("replace into bid (%s) values (%s)", insert.f0, insert.f1));
                    columnMaps.clear();
                }
            }
            if (!columnMaps.isEmpty()) {
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMaps);
                gauss.update(String.format("replace into bid (%s) values (%s)", insert.f0, insert.f1));
                columnMaps.clear();
            }
        }
    }
}
