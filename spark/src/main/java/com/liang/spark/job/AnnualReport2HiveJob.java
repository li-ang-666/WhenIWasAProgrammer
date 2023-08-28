package com.liang.spark.job;

import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import org.apache.spark.sql.SparkSession;

public class AnnualReport2HiveJob {
    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        TableFactory.jdbc(spark, "gauss", "entity_annual_report_shareholder_equity_change_details")
                .createOrReplaceTempView("t3");
        TableFactory.jdbc(spark, "gauss", "entity_annual_report_shareholder_equity_details")
                .createOrReplaceTempView("t4");
        TableFactory.jdbc(spark, "gauss", "entity_annual_report_investment_details")
                .createOrReplaceTempView("t6");
        TableFactory.jdbc(spark, "gauss", "entity_annual_report_ebusiness_details")
                .createOrReplaceTempView("t7");

        TableFactory.jdbc(spark, "463.bdp_equity", "entity_controller_details")
                .createOrReplaceTempView("t");
        TableFactory.jdbc(spark, "463.bdp_equity", "entity_beneficiary_details")
                .createOrReplaceTempView("tt");
        TableFactory.jdbc(spark, "463.bdp_equity", "shareholder_identity_type_details")
                .createOrReplaceTempView("ttt");

        spark.sql("insert overwrite table test.entity_annual_report_shareholder_equity_change_details select /*+ REPARTITION(180) */ * from t3");
        spark.sql("insert overwrite table test.entity_annual_report_shareholder_equity_details select /*+ REPARTITION(360) */ * from t4");
        spark.sql("insert overwrite table test.entity_annual_report_investment_details select /*+ REPARTITION(180) */ * from t6");
        spark.sql("insert overwrite table test.entity_annual_report_ebusiness_details select /*+ REPARTITION(180) */ * from t7");

        spark.sql("insert overwrite table test.entity_controller_details select /*+ REPARTITION(180) */ * from t");
        spark.sql("insert overwrite table test.entity_beneficiary_details select /*+ REPARTITION(180) */ * from tt");
        spark.sql("insert overwrite table test.shareholder_identity_type_details select /*+ REPARTITION(180) */ * from ttt");
    }
}
