package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class BlockPartnershipJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        //table
        TableFactory.jdbc(spark, "463.bdp_equity", "shareholder_identity_type_details")
                .createOrReplaceTempView("t_shareholder_identity_type_details");
        TableFactory.jdbc(spark, "465.company_base", "tyc_entity_general_property_reference")
                .createOrReplaceTempView("t_tyc_entity_general_property_reference");
        //sql
        String sql1 = new SQL().SELECT("distinct tyc_unique_entity_id company_id")
                .FROM("t_shareholder_identity_type_details")
                .WHERE("entity_name_valid is null or entity_name_valid = '' or entity_name_valid_with_shareholder_identity_type is null or entity_name_valid_with_shareholder_identity_type = ''")
                .toString();
        String sql2 = new SQL().SELECT("distinct tyc_unique_entity_id company_id")
                .FROM("t_tyc_entity_general_property_reference")
                .WHERE("entity_property in (15,16)")
                .toString();
        //exec
        spark.sql(sql1).unionAll(spark.sql(sql2))
                .persist(StorageLevel.MEMORY_AND_DISK())
                .createOrReplaceTempView("t");
        long count = spark.sql("select 1 from t").count();
        spark.sql("select company_id from t").repartition(new BigDecimal(count / 128L).intValue())
                .foreachPartition(new BlockPartnershipSink(ConfigUtils.getConfig()));
    }

    @Slf4j
    @RequiredArgsConstructor
    private final static class BlockPartnershipSink implements ForeachPartitionFunction<Row> {
        private final Config config;

        @Override
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
            }
        }
    }
}
