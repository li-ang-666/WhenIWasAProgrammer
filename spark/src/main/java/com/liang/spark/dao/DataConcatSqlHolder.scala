package com.liang.spark.dao

class DataConcatSqlHolder {
  def RestrictedConsumptionMostApplicantSql(): String = {
    """
      |-- 限制消费最多的申请人
      |with t as(
      |select t1.company_id,t1.case_create_time,tmp_tb.tmp_col applicant_name
      |from ods_judicial_risk_restrict_consumption_split_index_df t1
      |lateral view explode(split(if(applicant_inlink is not null and applicant_inlink <> '', applicant_inlink, applicant),';')) tmp_tb as tmp_col
      |where t1.company_id<>0 and t1.is_history=0
      |and t1.applicant is not null and t1.applicant<>''
      |),
      |tt as(
      |select company_id,applicant_name,count(1) cnt,max(case_create_time) mx
      |from t group by company_id,applicant_name
      |),
      |ttt as(
      |select company_id,applicant_name,row_number()over(partition by company_id order by cnt desc,mx desc) rn
      |from tt
      |)
      |select * from ttt where rn=1 limit 11111
      |
      |""".stripMargin
  }


}
