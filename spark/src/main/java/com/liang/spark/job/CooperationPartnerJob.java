package com.liang.spark.job;

import com.liang.common.util.ApolloUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

@Slf4j
public class CooperationPartnerJob {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSparkWithHudi(args);
        spark.udf().register("fmt", new FormatIdentity(), DataTypes.StringType);
        String sqls = ApolloUtils.get("cooperation-partner.sql");
        for (String sql : sqls.split(";")) {
            if (StringUtils.isBlank(sql)) continue;
            log.info("sql: {}", sql);
            spark.sql(sql);
        }
    }

    private static final class FormatIdentity implements UDF1<String, String> {
        @Override
        public String call(String identity) {
            return identity
                    .replaceAll("（", "(")
                    .replaceAll("）", ")")
                    .replaceAll("。|\\.|；|;|，|,|\\\\|(、+)", "、")
                    .replaceAll("(^、)|(、$)|\\s", "");
        }
    }
}
