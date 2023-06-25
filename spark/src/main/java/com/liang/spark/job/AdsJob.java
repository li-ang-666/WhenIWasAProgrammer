package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.SneakyThrows;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AdsJob {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        spark.sql("use ads");
        spark.sql("select * from ads_user_tag_commercial_df where pt='20230624'")
                .repartition(100)
                .foreachPartition(new Sink(ConfigUtils.getConfig()));
        spark.stop();
    }

    private static class Sink implements ForeachPartitionFunction<Row> {
        private final Config config;

        public Sink(Config config) {
            this.config = config;
        }

        @Override
        @SneakyThrows
        public void call(Iterator<Row> rowIterator) throws Exception {
            ConfigUtils.setConfig(config);
            DorisTemplate dorisTemplate = new DorisTemplate("dorisSink");
            dorisTemplate.enableCache(1000 * 10, 102400);

            DorisSchema dorisSchema = DorisSchema.builder()
                    .database("test_db")
                    .tableName("ads_user_tag_commercial_df_tmp")
                    .uniqueOrderBy(DorisSchema.DEFAULT_UNIQUE_ORDER_BY)
                    .uniqueDeleteOn(DorisSchema.DEFAULT_UNIQUE_DELETE_ON)
                    .build();
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                String json = row.json();
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(json);
                columnMap.put("__DORIS_DELETE_SIGN__", 0);
                columnMap.put("__DORIS_SEQUENCE_COL__", System.currentTimeMillis());
                DorisOneRow dorisOneRow = new DorisOneRow(dorisSchema, columnMap);
                dorisTemplate.update(dorisOneRow);
            }
            TimeUnit.SECONDS.sleep(30);
        }
    }
}
