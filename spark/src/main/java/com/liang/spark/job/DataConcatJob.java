package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.HbaseOneRow;
import com.liang.common.util.ConfigUtils;
import com.liang.spark.basic.HbaseSink;
import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.dao.RestrictedConsumptionMostApplicant;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class DataConcatJob {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        Config config = ConfigUtils.getConfig();

        spark.sql("use ods");
        spark.sql(preQuery(RestrictedConsumptionMostApplicant.queryMostApplicant(false)))
                .foreachPartition(new HbaseSink(config, row -> {
                    String companyId = String.valueOf(row.get(0));
                    String concat = String.valueOf(row.get(1));
                    String type;
                    String id;
                    String name;
                    if (concat.matches(".*?:\\d+")) {
                        type = "1";
                        String[] split = concat.split(":");
                        id = split[1];
                        name = split[0];
                    } else {
                        type = null;
                        id = null;
                        name = concat;
                    }
                    return new HbaseOneRow("dataConcatOffline", companyId)
                            .put("test_restrict_consumption_most_applicant_type", type)
                            .put("test_restrict_consumption_most_applicant_id", id)
                            .put("test_restrict_consumption_most_applicant_name", name);
                }));

        spark.close();
    }

    private static String preQuery(String sql) {
        log.warn("{}", sql);
        return sql;
    }
}
