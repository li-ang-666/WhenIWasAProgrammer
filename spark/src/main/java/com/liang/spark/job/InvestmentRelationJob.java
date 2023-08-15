package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.AbstractSQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.project.investment.relation.InvestmentRelationService;
import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InvestmentRelationJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        TableFactory.jdbc(spark, "435.company_base", "company_index")
                .createOrReplaceTempView("company_index");
        spark.sql("select distinct company_id from company_index")
                .repartition(1200)
                .foreachPartition(new InvestmentRelationForeachPartitionSink(ConfigUtils.getConfig()));
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class InvestmentRelationForeachPartitionSink implements ForeachPartitionFunction<Row> {
        private final Config config;

        @Override
        public void call(Iterator<Row> t) throws Exception {
            ConfigUtils.setConfig(config);
            InvestmentRelationService service = new InvestmentRelationService();
            JdbcTemplate jdbcTemplate = new JdbcTemplate("457.prism_shareholder_path");
            ArrayList<String> list = new ArrayList<>();
            while (t.hasNext()) {
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(t.next().json());
                String companyId = String.valueOf(columnMap.get("company_id"));
                List<String> sqls = service.invoke(companyId).stream().map(AbstractSQL::toString).collect(Collectors.toList());
                list.addAll(sqls);
                if (list.size() >= 64) {
                    jdbcTemplate.update(list);
                    list.clear();
                }
            }
            jdbcTemplate.update(list);
            list.clear();
        }
    }
}
