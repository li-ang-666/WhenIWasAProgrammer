package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.project.ratio.path.company.RatioPathCompanyService;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.RequiredArgsConstructor;
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
        String sql = new SQL().SELECT("distinct tyc_unique_entity_id company_id")
                .FROM("ads.ads_bdp_equity_shareholder_identity_type_details")
                .WHERE("pt = '20231101'")
                .toString();
        spark.sql(sql)
                .repartition(3600)
                .foreachPartition(new RatioPathCompanyForeach(ConfigUtils.getConfig()));
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class RatioPathCompanyForeach implements ForeachPartitionFunction<Row> {
        private final Config config;

        @Override
        public void call(Iterator<Row> iterator) {
            ConfigUtils.setConfig(config);
            RatioPathCompanyService service = new RatioPathCompanyService();
            Set<Long> set = new HashSet<>();
            while (iterator.hasNext()) {
                String json = iterator.next().json();
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(json);
                String companyIdInvested = String.valueOf(columnMap.get("company_id"));
                if (StringUtils.isNumeric(companyIdInvested) && !"0".equals(companyIdInvested)) {
                    set.add(Long.parseLong(companyIdInvested));
                }
                if (set.size() >= 128) {
                    service.invoke(set);
                    set.clear();
                }
            }
            service.invoke(set);
        }
    }
}
