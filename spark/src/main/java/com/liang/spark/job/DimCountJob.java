package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.dim.count.impl.RatioPathCompany;
import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;
import java.util.Map;

public class DimCountJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        TableFactory.jdbc(spark, "435.company_base", "company_index")
                .createOrReplaceTempView("t1");
        TableFactory.jdbc(spark, "040.human_base", "human")
                .createOrReplaceTempView("t2");
        String sql1 = "select distinct 'company' type,company_id id from t1";
        String sql2 = "select distinct 'shareholder' type,human_id id from t2";
        spark.sql(String.format("%s union all %s", sql1, sql2))
                .repartition(2400)
                .foreachPartition(new DimCountForeachPartitionSink(ConfigUtils.getConfig()));
    }

    @RequiredArgsConstructor
    private final static class DimCountForeachPartitionSink implements ForeachPartitionFunction<Row> {
        private final Config config;

        @Override
        public void call(Iterator<Row> t) {
            ConfigUtils.setConfig(config);
            RatioPathCompany ratioPathCompany = new RatioPathCompany();
            SingleCanalBinlog singleCanalBinlog = new SingleCanalBinlog();
            HbaseTemplate hbaseTemplate = new HbaseTemplate("hbaseSink");
            hbaseTemplate.enableCache();
            while (t.hasNext()) {
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(t.next().json());
                if (String.valueOf(columnMap.get("type")).equals("company")) {
                    columnMap.put("company_id", String.valueOf(columnMap.get("id")));
                    columnMap.put("shareholder_id", String.valueOf(columnMap.get("id")));
                } else {
                    columnMap.put("shareholder_id", String.valueOf(columnMap.get("id")));
                }
                singleCanalBinlog.setColumnMap(columnMap);
                hbaseTemplate.update(ratioPathCompany.updateWithReturn(singleCanalBinlog));
            }
            hbaseTemplate.flush();
        }
    }
}
