package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ApolloUtils;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DateTimeUtils;
import com.liang.common.util.JsonUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;
import java.util.Map;

@Slf4j
public class CooperationPartnerNewJob {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSparkWithHudi(args);
        spark.udf().register("format_reg_st", new FormatRegSt(), DataTypes.IntegerType);
        spark.udf().register("format_identity", new FormatIdentity(), DataTypes.StringType);
        spark.udf().register("format_ratio", new FormatRatio(), DataTypes.StringType);
        String pt = DateTimeUtils.getLastNDateTime(1, "yyyyMMdd");
        // 写入 hive 正式表 当前分区
        spark.sql(String.format("alter table hudi_ads.cooperation_partner_new drop if exists partition (pt = %s)", pt));
        do {
            String sql1 = ApolloUtils.get("cooperation-partner-new.sql").replaceAll("\\$pt", pt);
            log.info("sql1: {}", sql1);
            spark.sql(sql1);
        } while (spark.table("hudi_ads.cooperation_partner_new").where("pt = " + pt).count() < 700_000_000L);
        // 写入 hive diff表 当前分区
        spark.sql(String.format("alter table hudi_ads.cooperation_partner_diff drop if exists partition (pt = %s)", pt));
        do {
            String sql2 = ApolloUtils.get("cooperation-partner-diff.sql").replaceAll("\\$pt", pt);
            log.info("sql2: {}", sql2);
            spark.sql(sql2);
        } while (spark.table("hudi_ads.cooperation_partner_diff").where("pt = " + pt).count() < 1);
        // 写入 rds
        spark.table("hudi_ads.cooperation_partner_diff")
                .where("pt = " + pt)
                .orderBy(new Column("boss_human_pid"), new Column("partner_human_pid"), new Column("company_gid"))
                .foreachPartition(new CooperationPartnerSink(ConfigUtils.getConfig()));
        // 写入 hive 正式表 1号分区
        spark.sql("insert overwrite table hudi_ads.cooperation_partner_new partition(pt = 1) select * from hudi_ads.cooperation_partner_new where pt = " + pt);
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
        private final static int BATCH_SIZE = 512;
        private final Config config;

        @Override
        public void call(Iterator<Row> iterator) {
            ConfigUtils.setConfig(config);
            JdbcTemplate jdbcTemplate = new JdbcTemplate("gauss");
            while (iterator.hasNext()) {
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(iterator.next().json());
            }
        }
    }
}
