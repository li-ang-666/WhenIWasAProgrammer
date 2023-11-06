package com.liang.spark.job;

import com.liang.common.util.DateTimeUtils;
import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import org.apache.spark.sql.SparkSession;

public class PxCheckJob {
    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
        String pt = DateTimeUtils.getLastNDateTime(1, "yyyyMMdd");
        SparkSession spark = SparkSessionFactory.createSpark(args);
        //股东
        TableFactory.jdbc(spark, "457.prism_shareholder_path", "ratio_path_company")
                .createOrReplaceTempView("t1");
        TableFactory.jdbc(spark, "463.bdp_equity", "entity_controller_details")
                .createOrReplaceTempView("t2");
        TableFactory.jdbc(spark, "463.bdp_equity", "entity_beneficiary_details")
                .createOrReplaceTempView("t3");
        TableFactory.jdbc(spark, "463.bdp_equity", "shareholder_identity_type_details")
                .createOrReplaceTempView("t4");
        spark.sql("insert overwrite table ads.ads_prism_shareholder_path_ratio_path_company partition(pt = '" + pt + "') select /*+ REPARTITION(360) */ * from t1");
        spark.sql("insert overwrite table ads.ads_bdp_equity_entity_controller_details partition(pt = '" + pt + "') select /*+ REPARTITION(360) */ * from t2");
        spark.sql("insert overwrite table ads.ads_bdp_equity_entity_beneficiary_details partition(pt = '" + pt + "') select /*+ REPARTITION(360) */ * from t3");
        spark.sql("insert overwrite table ads.ads_bdp_equity_shareholder_identity_type_details partition(pt = '" + pt + "') select /*+ REPARTITION(360) */ * from t4");
        //年报
        TableFactory.jdbc(spark, "gauss", "entity_annual_report_shareholder_equity_change_details")
                .createOrReplaceTempView("t_3");
        TableFactory.jdbc(spark, "gauss", "entity_annual_report_shareholder_equity_details")
                .createOrReplaceTempView("t_4");
        TableFactory.jdbc(spark, "gauss", "entity_annual_report_investment_details")
                .createOrReplaceTempView("t_6");
        TableFactory.jdbc(spark, "gauss", "entity_annual_report_ebusiness_details")
                .createOrReplaceTempView("t_7");
        spark.sql("insert overwrite table ads.ads_company_base_entity_annual_report_shareholder_equity_change_details partition(pt = '" + pt + "') select /*+ REPARTITION(360) */ * from t_3");
        spark.sql("insert overwrite table ads.ads_company_base_entity_annual_report_shareholder_equity_details partition(pt = '" + pt + "') select /*+ REPARTITION(360) */ * from t_4");
        spark.sql("insert overwrite table ads.ads_company_base_entity_annual_report_investment_details partition(pt = '" + pt + "') select /*+ REPARTITION(360) */ * from t_6");
        spark.sql("insert overwrite table ads.ads_company_base_entity_annual_report_ebusiness_details partition(pt = '" + pt + "') select /*+ REPARTITION(360) */ * from t_7");
        // 工商
        TableFactory.jdbc(spark, "469.entity_operation_development", "entity_mainland_general_registration_info_details")
                .createOrReplaceTempView("t_company");
        TableFactory.jdbc(spark, "469.entity_operation_development", "entity_mainland_public_institution_registration_info_details")
                .createOrReplaceTempView("t_gov");
        spark.sql("insert overwrite table ads.ads_entity_operation_development_entity_mainland_general_registration_info_details partition(pt = '" + pt + "') select /*+ REPARTITION(360) */ * from t_company");
        spark.sql("insert overwrite table ads.ads_entity_operation_development_entity_mainland_public_institution_registration_info_details partition(pt = '" + pt + "') select /*+ REPARTITION(180) */ * from t_gov");
    }
}
