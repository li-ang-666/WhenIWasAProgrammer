package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AdsJob {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        spark.sql("use ads");
        spark.sql("select * from ads_user_tag_commercial_df where pt='20230625'")
                .repartition(600)
                .foreachPartition(new Sink(ConfigUtils.getConfig()));
        spark.stop();
    }

    @RequiredArgsConstructor
    private static class Sink implements ForeachPartitionFunction<Row> {
        private final static Set<String> DIMENSION_KEYS = new HashSet<String>() {{
            add("...");
        }};
        private final Config config;

        @Override
        public void call(Iterator<Row> rowIterator) {
            ConfigUtils.setConfig(config);
            DorisTemplate dorisTemplate = new DorisTemplate("dorisSink");
            dorisTemplate.enableCache();
            // config `delete_on` and `seq_col` if is unique
            DorisSchema dorisSchema = DorisSchema.builder()
                    .database("ads")
                    .tableName("ads_user_tag_commercial").build();
            while (rowIterator.hasNext()) {
                String json = rowIterator.next().json();
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(json);
                // filter needed dimension columns
                Map<String, Object> sinkMap = columnMap.entrySet().stream()
                        .filter(entry -> DIMENSION_KEYS.contains(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                dorisTemplate.update(new DorisOneRow(dorisSchema, sinkMap));
            }
            dorisTemplate.flush();
        }
    }
}
