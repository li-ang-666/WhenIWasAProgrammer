package com.liang.spark.job;

import com.liang.common.util.JsonUtils;
import com.liang.flink.project.ratio.path.company.RatioPathCompanyService;
import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class RatioPathCompanyJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        TableFactory.jdbc(spark, "prismShareholderPath", "investment_relation")
                .createOrReplaceTempView("t");
        spark.sql("select distinct company_id_invested from t")
                .repartition(1200)
                .foreachPartition(new RatioPathCompanyForeach());
    }

    @Slf4j
    private final static class RatioPathCompanyForeach implements ForeachPartitionFunction<Row> {

        @Override
        public void call(Iterator<Row> iterator) throws Exception {
            RatioPathCompanyService service = new RatioPathCompanyService();
            Set<Long> set = new HashSet<>();
            while (iterator.hasNext()) {
                String json = iterator.next().json();
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(json);
                String companyIdInvested = String.valueOf(columnMap.get("company_id_invested"));
                if (StringUtils.isNumeric(companyIdInvested)) {
                    set.add(Long.parseLong(companyIdInvested));
                }
                if (set.size() >= 1024) {
                    try {
                        service.invoke(set);
                    } catch (Exception e) {
                        log.error("trigger({})", set);
                    }
                    set.clear();
                }
            }
            try {
                service.invoke(set);
            } catch (Exception e) {
                log.error("trigger({})", set);
            }
        }
    }
}
