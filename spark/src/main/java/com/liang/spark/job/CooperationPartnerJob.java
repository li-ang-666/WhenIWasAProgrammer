package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.database.template.RedisTemplate;
import com.liang.common.util.*;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
public class CooperationPartnerJob {
    private final static String REDIS_KEY = "cooperation_partner";
    private final static String STEP_1_START = "step_1_start";
    private final static String STEP_2_START = "step_2_start";
    private final static String END = "end";

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSparkWithHudi(args);
        Config config = ConfigUtils.getConfig();
        RedisTemplate redis = new RedisTemplate("metadata");
        spark.udf().register("format_reg_st", new FormatRegSt(), DataTypes.IntegerType);
        spark.udf().register("format_identity", new FormatIdentity(), DataTypes.StringType);
        spark.udf().register("format_ratio", new FormatRatio(), DataTypes.StringType);
        String pt = DateUtils.getOfflinePt(1, "yyyyMMdd");
        // step1
        String redisValue = redis.get(REDIS_KEY);
        if (!STEP_2_START.equals(redisValue)) {
            redis.set(REDIS_KEY, STEP_1_START);
            // 写入 hive 正式表 当前分区
            String sql1 = ApolloUtils.get("cooperation-partner.sql").replaceAll("\\$pt", pt);
            log.info("sql1: {}", sql1);
            spark.sql(sql1);
            assert spark.table("hudi_ads.cooperation_partner").where("pt = " + pt).count() > 700_000_000L;
            // 写入 hive diff表 当前分区
            String sql2 = ApolloUtils.get("cooperation-partner-diff.sql").replaceAll("\\$pt", pt);
            log.info("sql2: {}", sql2);
            spark.sql(sql2);
            assert spark.table("hudi_ads.cooperation_partner_diff").where("pt = " + pt).count() > 0;
        }
        // step2
        redis.set(REDIS_KEY, STEP_2_START);
        // 写入 rds
        spark.table("hudi_ads.cooperation_partner_diff")
                .where("pt = " + pt)
                .repartition(8, new Column("boss_human_pid"))
                .sortWithinPartitions(new Column("boss_human_pid"), new Column("partner_human_pid"), new Column("company_gid"))
                .foreachPartition(new CooperationPartnerSink(config));
        // 写入 hive 正式表 0号分区
        spark.table("hudi_ads.cooperation_partner").where("pt = " + pt).drop("pt").createOrReplaceTempView("current");
        spark.sql("insert overwrite table hudi_ads.cooperation_partner partition(pt = 0) select * from current");
        Dataset<Row> tb = spark.table("hudi_ads.cooperation_partner");
        assert tb.where("pt = " + pt).count() == tb.where("pt = 0").count();
        redis.set(REDIS_KEY, END);
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
                    .replace("_", ".")
                    .replaceAll("、+", "、")
                    .replaceAll("(^、)|(、$)|(\\d@)", "");
        }
    }

    private static final class FormatRegSt implements UDF1<String, Integer> {
        private final static String[] OK = new String[]{
                "存续", "开业", "登记", "在业", "在营", "正常", "经营", "在营在册", "有效", "在业在册",
                "迁入", "迁出", "迁他县市",
                "成立中", "设立中", "正常执业", "仍注册",
                "核准设立", "设立许可", "核准许可登记", "核准认许", "核准报备"
        };

        @Override
        public Integer call(String regSt) {
            return StringUtils.equalsAny(String.valueOf(regSt), OK) ? 1 : 0;
        }
    }

    private static final class FormatRatio implements UDF1<Object, String> {
        private final static BigDecimal pivot = new BigDecimal("100");

        @Override
        public String call(Object ratio) {
            return new BigDecimal(String.valueOf(ratio))
                    .multiply(pivot)
                    .setScale(4, RoundingMode.DOWN)
                    .stripTrailingZeros()
                    .toPlainString()
                    .replace(".", "_")
                    + "%";
        }
    }

    @RequiredArgsConstructor
    private final static class CooperationPartnerSink implements ForeachPartitionFunction<Row> {
        private final static String TEMPLATE = " ON DUPLICATE KEY UPDATE" +
                " boss_human_gid = VALUES(boss_human_gid)," +
                " boss_human_name = VALUES(boss_human_name)," +
                " boss_identity = VALUES(boss_identity)," +
                " boss_shares = VALUES(boss_shares)," +
                " company_name = VALUES(company_name)," +
                " company_registered_status = VALUES(company_registered_status)," +
                " company_registered_capital = VALUES(company_registered_capital)," +
                " s = VALUES(s)," +
                " partner_human_gid = VALUES(partner_human_gid)," +
                " partner_human_name = VALUES(partner_human_name)," +
                " partner_identity = VALUES(partner_identity)," +
                " partner_shares = VALUES(partner_shares)," +
                " single_cooperation_score = VALUES(single_cooperation_score)," +
                " multi_cooperation_score = VALUES(multi_cooperation_score)," +
                " single_cooperation_row_number = VALUES(single_cooperation_row_number)," +
                " multi_cooperation_dense_rank = VALUES(multi_cooperation_dense_rank)," +
                " cooperation_times_with_this_partner = VALUES(cooperation_times_with_this_partner)," +
                " cooperation_times_with_all_partner = VALUES(cooperation_times_with_all_partner)," +
                " total_partners = VALUES(total_partners)," +
                " update_time = NOW()";
        private final static int BATCH_SIZE = 128;
        private final Config config;

        @Override
        public void call(Iterator<Row> iterator) {
            ConfigUtils.setConfig(config);
            JdbcTemplate jdbcTemplate = new JdbcTemplate("467.company_base");
            List<Map<String, Object>> columnMaps = new ArrayList<>(BATCH_SIZE);
            while (iterator.hasNext()) {
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(iterator.next().json());
                Object obj = columnMap.get("column_map");
                if (obj == null) {
                    String delete = new SQL().DELETE_FROM("cooperation_partner")
                            .WHERE("boss_human_pid = " + SqlUtils.formatValue(String.valueOf(columnMap.get("boss_human_pid"))))
                            .WHERE("partner_human_pid = " + SqlUtils.formatValue(String.valueOf(columnMap.get("partner_human_pid"))))
                            .WHERE("company_gid = " + SqlUtils.formatValue(String.valueOf(columnMap.get("company_gid"))))
                            .toString();
                    jdbcTemplate.update(delete);
                } else {
                    columnMaps.add(JsonUtils.parseJsonObj(String.valueOf(obj)));
                    if (columnMaps.size() >= BATCH_SIZE) {
                        Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMaps);
                        String sql = new SQL().INSERT_INTO("cooperation_partner")
                                .INTO_COLUMNS(insert.f0)
                                .INTO_VALUES(insert.f1)
                                .toString() + TEMPLATE;
                        jdbcTemplate.update(sql);
                        columnMaps.clear();
                    }
                }
            }
            if (!columnMaps.isEmpty()) {
                Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMaps);
                String sql = new SQL().INSERT_INTO("cooperation_partner")
                        .INTO_COLUMNS(insert.f0)
                        .INTO_VALUES(insert.f1)
                        .toString() + TEMPLATE;
                jdbcTemplate.update(sql);
                columnMaps.clear();
            }
        }
    }
}
