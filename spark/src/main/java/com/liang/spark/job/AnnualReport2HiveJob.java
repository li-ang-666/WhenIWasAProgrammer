package com.liang.spark.job;

import com.liang.spark.basic.FixedMySQLDialect;
import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;

public class AnnualReport2HiveJob {
    public static void main(String[] args) {
        JdbcDialects.unregisterDialect(JdbcDialects.get("jdbc:mysql"));
        JdbcDialects.registerDialect(new FixedMySQLDialect());
        SparkSession spark = SparkSessionFactory.createSpark(args);
        TableFactory.jdbc(spark, "gauss", "entity_annual_report_shareholder_equity_change_details")
                .createOrReplaceTempView("t3");
        TableFactory.jdbc(spark, "gauss", "entity_annual_report_shareholder_equity_details")
                .createOrReplaceTempView("t4");
        TableFactory.jdbc(spark, "gauss", "entity_annual_report_investment_details")
                .createOrReplaceTempView("t6");
        TableFactory.jdbc(spark, "gauss", "entity_annual_report_ebusiness_details")
                .createOrReplaceTempView("t7");

        spark.sql("insert overwrite table test.entity_annual_report_shareholder_equity_change_details select /*+ REPARTITION(180) */ cast(`id` as bigint) `id`,cast(`annual_report_year` as string) `annual_report_year`,cast(`tyc_unique_entity_id` as string) `tyc_unique_entity_id`,cast(`entity_name_valid` as string) `entity_name_valid`,cast(`entity_type_id` as string) `entity_type_id`,cast(`annual_report_tyc_unique_entity_id_shareholder` as string) `annual_report_tyc_unique_entity_id_shareholder`,cast(`annual_report_entity_name_valid_shareholder` as string) `annual_report_entity_name_valid_shareholder`,cast(`annual_report_entity_name_register_shareholder` as string) `annual_report_entity_name_register_shareholder`,cast(`annual_report_entity_type_id_shareholder` as string) `annual_report_entity_type_id_shareholder`,cast(`annual_report_equity_ratio_before_change` as string) `annual_report_equity_ratio_before_change`,cast(`annual_report_equity_ratio_after_change` as string) `annual_report_equity_ratio_after_change`,cast(`annual_report_equity_ratio_change_time` as string) `annual_report_equity_ratio_change_time`,cast(`is_display_annual_report_equity_ratio_change` as string) `is_display_annual_report_equity_ratio_change`,cast(`delete_status` as string) `delete_status`,cast(`create_time` as string) `create_time`,cast(`update_time` as string) `update_time` from t3");
        spark.sql("insert overwrite table test.entity_annual_report_shareholder_equity_details select /*+ REPARTITION(180) */ cast(`id` as bigint) `id`,cast(`business_id` as string) `business_id`,cast(`annual_report_year` as string) `annual_report_year`,cast(`tyc_unique_entity_id` as string) `tyc_unique_entity_id`,cast(`entity_name_valid` as string) `entity_name_valid`,cast(`entity_type_id` as string) `entity_type_id`,cast(`annual_report_tyc_unique_entity_id_shareholder` as string) `annual_report_tyc_unique_entity_id_shareholder`,cast(`annual_report_entity_name_valid_shareholder` as string) `annual_report_entity_name_valid_shareholder`,cast(`annual_report_entity_name_register_shareholder` as string) `annual_report_entity_name_register_shareholder`,cast(`annual_report_entity_type_id_shareholder` as string) `annual_report_entity_type_id_shareholder`,cast(`annual_report_shareholder_capital_type` as string) `annual_report_shareholder_capital_type`,cast(`annual_report_shareholder_capital_source` as string) `annual_report_shareholder_capital_source`,cast(`annual_report_shareholder_equity_amt` as string) `annual_report_shareholder_equity_amt`,cast(`annual_report_shareholder_equity_currency` as string) `annual_report_shareholder_equity_currency`,cast(`annual_report_shareholder_equity_valid_date` as string) `annual_report_shareholder_equity_valid_date`,cast(`annual_report_shareholder_equity_submission_method` as string) `annual_report_shareholder_equity_submission_method`,cast(`is_display_annual_report_shareholder_equity_details` as string) `is_display_annual_report_shareholder_equity_details`,cast(`delete_status` as string) `delete_status`,cast(`create_time` as string) `create_time`,cast(`update_time` as string) `update_time` from t4");
        spark.sql("insert overwrite table test.entity_annual_report_investment_details select /*+ REPARTITION(180) */ * from t6");
        spark.sql("insert overwrite table test.entity_annual_report_ebusiness_details select /*+ REPARTITION(180) */ * from t7");
    }
}
