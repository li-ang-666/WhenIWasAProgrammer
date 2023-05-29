package com.liang.spark.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class DataConcat {
    public static void main(String[] args) {
        SparkSession ssc = SparkSession
                .builder()
                .config("spark.debug.maxToStringFields", "200")
                .enableHiveSupport()
                .getOrCreate();
        ssc.sql("use ods");
        long count = ssc.sql("with t as(\n" +
                "    select company_id,if(applicant_inlink is not null and applicant_inlink <> '', applicant_inlink, applicant) fi, count(1) cnt\n" +
                "from ods_judicial_risk_restrict_consumption_split_index_df\n" +
                "where applicant <> ''\n" +
                "  and is_history = 0\n" +
                "   -- and company_id = 14427175\n" +
                "group by company_id,if(applicant_inlink is not null and applicant_inlink <> '', applicant_inlink, applicant)\n" +
                "order by cnt desc\n" +
                "),tt as(\n" +
                "    select company_id,fi,row_number() over(partition by company_id order by cnt desc) rn from t\n" +
                "    )\n" +
                "select * from tt where rn = 1\n" +
                "limit 10000").count();
        log.error("count: {}", count);
        ssc.close();
    }
}
