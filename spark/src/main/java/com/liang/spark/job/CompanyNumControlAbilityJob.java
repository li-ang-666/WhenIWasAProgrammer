package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;
import java.util.Map;

public class CompanyNumControlAbilityJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        TableFactory.jdbc(spark, "457.prism_shareholder_path", "ratio_path_company")
                //.where("investment_ratio_total >= 0.05")
                .where("shareholder_entity_type = 1")
                //.where("shareholder_id is not null and shareholder_id <> '' and shareholder_id <> 0")
                .createOrReplaceTempView("ttt");
        spark.sql("select shareholder_id, count(1) cnt from ttt group by shareholder_id")
                .repartition(256)
                .foreachPartition(new CompanyNumControlAbilitySink(ConfigUtils.getConfig()));
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class CompanyNumControlAbilitySink implements ForeachPartitionFunction<Row> {
        private final Config config;

        @Override
        public void call(Iterator<Row> t) {
            ConfigUtils.setConfig(config);
            HbaseTemplate hbaseSink = new HbaseTemplate("hbaseSink");
            hbaseSink.enableCache(1000 * 60, 10240);
            while (t.hasNext()) {
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(t.next().json());
                String shareholderId = String.valueOf(columnMap.get("shareholder_id"));
                String cnt = String.valueOf(columnMap.get("cnt"));
                HbaseOneRow hbaseOneRow = new HbaseOneRow(HbaseSchema.COMPANY_ALL_COUNT, shareholderId);
                hbaseOneRow.put("num_control_ability", cnt);
                hbaseSink.update(hbaseOneRow);
                log.info("row: {}", hbaseOneRow);
            }
            hbaseSink.flush();
        }
    }
}
