package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.*;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.*;

@Slf4j
public class CooperationPartnerJob {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSparkWithHudi(args);
        Config config = ConfigUtils.getConfig();
        JdbcTemplate jdbcTemplate = new JdbcTemplate("gauss");
        spark.udf().register("fmt", new FormatIdentity(), DataTypes.StringType);
        String pt = DateTimeUtils.getLastNDateTime(1, "yyyyMMdd");
        Dataset<Row> table = spark.table("hudi_ads.cooperation_partner")
                .where("pt = " + pt)
                .where("multi_cooperation_dense_rank <= 20")
                .drop("pt");
        String argString = Arrays.toString(args);
        // step1 写 hive
        if (argString.contains("step1")) {
            // overwrite hive 分区
            spark.sql(ApolloUtils.get("cooperation-partner.sql").replace("${pt}", pt));
        }
        // step2 写 gauss
        if (argString.contains("step2")) {
            // hive 分区 数据量检查
            long count = table.count();
            if (count < 750_000_000L) {
                log.error("hive 分区 {}, 数据量 {}, 不合理", pt, count);
                return;
            } else {
                log.info("hive 分区 {}, 数据量 {}, 合理", pt, count);
            }
            // overwrite gauss 临时表
            for (int i = 0; i < 10; i++) {
                jdbcTemplate.update("drop table if exists company_base.cooperation_partner_" + i + "_tmp");
                jdbcTemplate.update("create table if not exists company_base.cooperation_partner_" + i + "_tmp like company_base.cooperation_partner_" + i);
            }
            table.repartition(256).foreachPartition(new CooperationPartnerSink(config));
            // gauss 表替换
            for (int i = 0; i < 10; i++) {
                jdbcTemplate.update("drop table if exists company_base.cooperation_partner_" + i);
                jdbcTemplate.update("alter table company_base.cooperation_partner_" + i + "_tmp rename company_base.cooperation_partner_" + i);
            }
        }
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
            HashMap<String, List<Map<String, Object>>> tableId2ColumnMaps = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                tableId2ColumnMaps.put(String.valueOf(i), new ArrayList<>(1024));
            }
            while (iterator.hasNext()) {
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(iterator.next().json());
                String tableId = String.valueOf(columnMap.remove("table_id"));
                List<Map<String, Object>> columnMaps = tableId2ColumnMaps.get(tableId);
                columnMaps.add(columnMap);
                if (columnMaps.size() >= 1024) {
                    Tuple2<String, String> insert = SqlUtils.columnMap2Insert(columnMaps);
                    String sql = String.format("insert into company_base.cooperation_partner_%s_tmp (%s) values (%s)", tableId, insert.f0, insert.f1);
                    jdbcTemplate.update(sql);
                    columnMaps.clear();
                }
            }
            tableId2ColumnMaps.forEach((k, v) -> {
                if (!v.isEmpty()) {
                    Tuple2<String, String> insert = SqlUtils.columnMap2Insert(v);
                    String sql = String.format("insert into company_base.cooperation_partner_%s_tmp (%s) values (%s)", k, insert.f0, insert.f1);
                    jdbcTemplate.update(sql);
                    v.clear();
                }
            });
        }
    }
}
