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
import java.util.Map;

public class DorisJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        spark.sql("select * from test.dwd_user_register_details_1")
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
                columnMap.put("__DORIS_DELETE_SIGN__", 0);
                DorisSchema schema = DorisSchema
                        .builder()
                        .database("dwd")
                        .tableName("dwd_user_register_details")
                        .uniqueDeleteOn("__DORIS_DELETE_SIGN__ = 1")
                        .build();
                DorisOneRow dorisOneRow = new DorisOneRow(schema, columnMap);
                doris.update(dorisOneRow);
            }
            doris.flush();
        }
    }
}
