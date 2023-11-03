package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class BlockPartnershipJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        //table
        TableFactory.jdbc(spark, "463.bdp_equity", "shareholder_identity_type_details")
                .createOrReplaceTempView("t_shareholder_identity_type_details");
        TableFactory.jdbc(spark, "465.company_base", "tyc_entity_general_property_reference")
                .createOrReplaceTempView("t_tyc_entity_general_property_reference");
        TableFactory.jdbc(spark, "457.prism_shareholder_path", "investment_relation")
                .createOrReplaceTempView("t_investment_relation");
        //sql
        String sql1 = new SQL().SELECT("distinct tyc_unique_entity_id company_id")
                .FROM("t_shareholder_identity_type_details")
                .WHERE("entity_name_valid is null or entity_name_valid = '' or entity_name_valid_with_shareholder_identity_type is null or entity_name_valid_with_shareholder_identity_type = ''")
                .toString();
        String sql2 = new SQL().SELECT("distinct tyc_unique_entity_id company_id")
                .FROM("t_tyc_entity_general_property_reference")
                .WHERE("entity_property in (15,16)")
                .toString();
        String sql3 = new SQL().SELECT("distinct company_id_invested company_id")
                .FROM("t_investment_relation")
                .WHERE("'2023-10-30 00:00:00' <= update_time and update_time <= '2023-11-03 23:00:00'")
                .toString();
        //exec
        spark.sql(sql1).unionAll(spark.sql(sql2)).unionAll(spark.sql(sql3))
                .createOrReplaceTempView("t");
        spark.sql("select distinct company_id from t")
                .repartition(10240000 / 128)
                .foreachPartition(new BlockPartnershipSink(ConfigUtils.getConfig()));
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class BlockPartnershipSink implements ForeachPartitionFunction<Row> {
        private final Config config;

        @Override
        @SneakyThrows
        public void call(Iterator<Row> t) {
            ConfigUtils.setConfig(config);
            List<String> ids = new ArrayList<>();
            while (t.hasNext()) {
                String companyId = String.valueOf(JsonUtils.parseJsonObj(t.next().json()).get("company_id"));
                ids.add(companyId);
            }
            log.info("ids: {}", ids);
            if (!ids.isEmpty()) {
                String sql = new SQL().UPDATE("investment_relation")
                        .SET("update_time = now()")
                        .WHERE("company_id_invested in " + ids.stream().collect(Collectors.joining(",", "(", ")")))
                        .toString();
                new JdbcTemplate("457.prism_shareholder_path").update(sql);
                TimeUnit.SECONDS.sleep(1);
            }
        }
    }
}
