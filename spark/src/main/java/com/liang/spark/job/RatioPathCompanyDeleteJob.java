package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.project.ratio.path.company.RatioPathCompanyDao;
import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;
import java.util.Map;

@Slf4j
public class RatioPathCompanyDeleteJob {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        TableFactory.jdbc(spark, "prismShareholderPath", "investment_relation")
                .createOrReplaceTempView("t1");
        TableFactory.jdbc(spark, "prismShareholderPath", "investment_relation")
                .createOrReplaceTempView("t2");
        spark.sql("select * from t1 union all select * from t2").limit(1).show();
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class RatioPathCompanyDeleteSink implements ForeachPartitionFunction<Row> {
        private final Config config;

        @Override
        public void call(Iterator<Row> t) throws Exception {
            ConfigUtils.setConfig(config);
            RatioPathCompanyDao dao = new RatioPathCompanyDao();
            while (t.hasNext()) {
                Row row = t.next();
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(row.json());
                long companyId = Long.parseLong(String.valueOf(columnMap.get("company_id")));
                dao.deleteAll(companyId);
            }
        }
    }
}
