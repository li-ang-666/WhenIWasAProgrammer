package com.liang.spark.job;

import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import org.apache.spark.sql.SparkSession;

public class PxCheckJob {
    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        // 年报
        //TableFactory.jdbc(spark, "gauss", "entity_annual_report_shareholder_equity_change_details")
        //        .createOrReplaceTempView("t3");
        //TableFactory.jdbc(spark, "gauss", "entity_annual_report_shareholder_equity_details")
        //        .createOrReplaceTempView("t4");
        //TableFactory.jdbc(spark, "gauss", "entity_annual_report_investment_details")
        //        .createOrReplaceTempView("t6");
        //TableFactory.jdbc(spark, "gauss", "entity_annual_report_ebusiness_details")
        //        .createOrReplaceTempView("t7");
        //spark.sql("insert overwrite table ads.ads_entity_operation_development_entity_annual_report_shareholder_equity_change_details partition(pt = '20230828') select /*+ REPARTITION(360) */ * from t3");
        //spark.sql("insert overwrite table ads.ads_entity_operation_development_entity_annual_report_shareholder_equity_details partition(pt = '20230828') select /*+ REPARTITION(360) */ * from t4");
        //spark.sql("insert overwrite table ads.ads_entity_operation_development_entity_annual_report_investment_details partition(pt = '20230828') select /*+ REPARTITION(360) */ * from t6");
        //spark.sql("insert overwrite table ads.ads_entity_operation_development_entity_annual_report_ebusiness_details partition(pt = '20230828') select /*+ REPARTITION(360) */ * from t7");

        // 股东
        //TableFactory.jdbc(spark, "463.bdp_equity", "entity_controller_details")
        //        .createOrReplaceTempView("t");
        //TableFactory.jdbc(spark, "463.bdp_equity", "entity_beneficiary_details")
        //        .createOrReplaceTempView("tt");
        //TableFactory.jdbc(spark, "463.bdp_equity", "shareholder_identity_type_details")
        //        .createOrReplaceTempView("ttt");
        //spark.sql("insert overwrite table ads.ads_bdp_equity_entity_controller_details partition(pt = '20230828') select /*+ REPARTITION(360) */ * from t");
        //spark.sql("insert overwrite table ads.ads_bdp_equity_entity_beneficiary_details partition(pt = '20230828') select /*+ REPARTITION(360) */ * from tt");
        //spark.sql("insert overwrite table ads.ads_bdp_equity_shareholder_identity_type_details partition(pt = '20230828') select /*+ REPARTITION(360) */ * from ttt");

        // 工商
        TableFactory.jdbc(spark, "469.entity_operation_development", "entity_annual_report_shareholder_equity_change_details")
                .createOrReplaceTempView("t3");
    }
}