package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ApolloUtils;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

@Slf4j
public class CooperationPartnerJob {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSparkWithHudi(args);
        Config config = ConfigUtils.getConfig();
        JdbcTemplate jdbcTemplate = new JdbcTemplate("gauss");
        spark.udf().register("fmt", new FormatIdentity(), DataTypes.StringType);
        // 写入hive临时表
        String sqls = ApolloUtils.get("cooperation-partner.sql");
        for (String sql : sqls.split(";")) {
            if (StringUtils.isBlank(sql)) continue;
            log.info("sql: {}", sql);
            spark.sql(sql);
        }
        Dataset<Row> tmp = spark.table("hudi_ads.cooperation_partner_tmp")
                .where("multi_cooperation_dense_rank <= 20");
        tmp.createOrReplaceTempView("tmp");
        long count = tmp.count();
        // hive 临时表 数据量检查
        if (count < 700_000_000L) {
            log.error("hive tmp 表, 数据量不合理");
            return;
        }
        // hive 临时表 覆盖主表
        spark.sql("insert overwrite table hudi_ads.cooperation_partner select * from tmp");
        // 重建 gauss 临时表
        jdbcTemplate.update("drop table if exists company_base.cooperation_partner_tmp");
        jdbcTemplate.update("create table if not exists company_base.cooperation_partner_tmp like company_base.cooperation_partner");
        // 写入 gauss 临时表
        tmp.repartition(256).foreachPartition(new CooperationPartnerSink(config));
        Long maxId = jdbcTemplate.queryForObject("select max(id) from company_base.cooperation_partner_tmp", rs -> rs.getLong(1));
        // gauss 临时表 数据量检查
        if (maxId < 700_000_000L) {
            log.error("gauss tmp 表, 数据量不合理");
            return;
        }
        // gauss 表替换
        jdbcTemplate.update(Arrays.asList(
                "drop table if exists company_base.cooperation_partner",
                "alter table company_base.cooperation_partner_tmp rename company_base.cooperation_partner"
        ));
    }

    private static final class FormatIdentity implements UDF1<String, String> {
        @Override
        public String call(String identity) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < identity.length(); i++) {
                char c = identity.charAt(i);
                if (c == '（')
                    builder.append('(');
                else if (c == '）')
                    builder.append(')');
                else if (c == '。' || c == '.' || c == '；' || c == ';' || c == '，' || c == ',' || c == '\\')
                    builder.append('、');
                else if (!Character.isWhitespace(c))
                    builder.append(c);
            }
            return builder.toString()
                    .replace("未知", "主要人员")
                    .replaceAll("、+", "、")
                    .replaceAll("(^、)|(、$)", "")
                    .replaceAll("(.*?)(股东\\(持股\\d)、(.*)", "$1$2.$3");
        }
    }

    @RequiredArgsConstructor
    private final static class CooperationPartnerSink implements ForeachPartitionFunction<Row> {
        private final Config config;

        @Override
        public void call(Iterator<Row> iterator) {
            ConfigUtils.setConfig(config);
            JdbcTemplate jdbcTemplate = new JdbcTemplate("gauss");
            jdbcTemplate.enableCache();
            while (iterator.hasNext()) {
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(iterator.next().json());
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMap);
                String sql = new SQL().INSERT_INTO("company_base.cooperation_partner_tmp")
                        .INTO_COLUMNS(insert.f0)
                        .INTO_VALUES(insert.f1)
                        .toString();
                jdbcTemplate.update(sql);
            }
            jdbcTemplate.flush();
        }
    }
}
