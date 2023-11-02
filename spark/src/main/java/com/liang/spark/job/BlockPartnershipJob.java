package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.RequiredArgsConstructor;
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
        String sql1 = new SQL().SELECT("distinct tyc_unique_entity_id company_id")
                .FROM("ads.ads_bdp_equity_shareholder_identity_type_details")
                .WHERE("pt = '20231101' and (entity_name_valid is null or entity_name_valid = '')")
                .toString();
        String sql2 = new SQL().SELECT("distinct tyc_unique_entity_id company_id")
                .FROM("ads.ads_company_base_tyc_entity_general_property_reference_df")
                .WHERE("pt = '20231101'")
                .WHERE("entity_property in (15,16)")
                .toString();

        spark.sql(sql1).unionAll(spark.sql(sql2))
                .persist(StorageLevel.DISK_ONLY())
                .createOrReplaceTempView("t");
        long count = spark.sql("select 1 from t").count();
        spark.sql("select company_id from t").repartition(new BigDecimal(count / 128L).intValue())
                .foreachPartition(new BlockPartnershipSink(ConfigUtils.getConfig()));
    }

    @RequiredArgsConstructor
    private final static class BlockPartnershipSink implements ForeachPartitionFunction<Row> {
        private final Config config;

        @Override
        public void call(Iterator<Row> t) throws Exception {
            ConfigUtils.setConfig(config);
            List<String> ids = new ArrayList<>();
            while (t.hasNext()) {
                String companyId = String.valueOf(JsonUtils.parseJsonObj(t.next().json()).get("company_id"));
                ids.add(companyId);
            }
            String sql = new SQL().UPDATE("investment_relation")
                    .SET("update_time = date_add(update_time, interval 1 second)")
                    .WHERE("company_id_invested in " + ids.stream().collect(Collectors.joining(",", "(", ")")))
                    .toString();
            new JdbcTemplate("457.prism_shareholder_path").update(sql);
        }
    }
}
