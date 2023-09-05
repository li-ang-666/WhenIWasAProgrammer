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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DorisJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        spark.sql("select * from test.test_ods_promotion_user_promotion_all_df_0830 where pt='20230903'")
                .repartition(180)
                .foreachPartition(new DorisForeachPartitionSink(ConfigUtils.getConfig()));
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class DorisForeachPartitionSink implements ForeachPartitionFunction<Row> {
        private final Config config;

        @Override
        public void call(Iterator<Row> iterator) {
            ConfigUtils.setConfig(config);
            DorisTemplate doris = new DorisTemplate("dorisSink");
            doris.enableCache();
            while (iterator.hasNext()) {
                Row row = iterator.next();
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(row.json());
                HashMap<String, Object> resultMap = new HashMap<>();
                resultMap.put("__DORIS_DELETE_SIGN__", 0);
                resultMap.put("promotion_code", columnMap.get("promotion_code"));
                resultMap.put("unique_user_id", columnMap.get("user_id"));
                resultMap.put("promotion_id", columnMap.get("promotion_id"));
                resultMap.put("use_status", columnMap.get("use_status"));
                resultMap.put("receive_time", columnMap.get("receive_time"));
                resultMap.put("effective_time", columnMap.get("effective_time"));
                resultMap.put("expiration_time", columnMap.get("expiration_time"));
                DorisSchema schema = DorisSchema
                        .builder()
                        .database("dwd")
                        .tableName("dwd_coupon_info")
                        .uniqueDeleteOn("__DORIS_DELETE_SIGN__ = 1")
                        .build();
                DorisOneRow dorisOneRow = new DorisOneRow(schema, resultMap);
                doris.update(dorisOneRow);
            }
            doris.flush();
        }
    }
}
