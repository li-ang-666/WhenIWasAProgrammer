package com.liang.spark.dao

import com.liang.common.util.DateTimeUtils

object RestrictedConsumptionMostApplicant {
  def queryMostApplicant(isHistory: Boolean): String = {
    s"""
       |-- 最多的限制消费令申请人
       |
       |with t as(
       |select t1.company_id,t1.case_create_time,t1.id,tmp_tb.tmp_col applicant_name
       |from ods_judicial_risk_restrict_consumption_split_index_df t1
       |lateral view explode(split(if(applicant_inlink is not null and applicant_inlink <> '', applicant_inlink, applicant),';')) tmp_tb as tmp_col
       |where t1.company_id<>0 and t1.pt='${DateTimeUtils.dateSub(DateTimeUtils.currentDate(), 1).replaceAll("-", "")}'
       |and t1.is_history=${if (isHistory) 1 else 0}
       |and t1.applicant is not null and t1.applicant<>''
       |and t1.dw_is_del =0
       |),
       |tt as(
       |select company_id,applicant_name,count(1) cnt,max(case_create_time) mxtime,max(id) mxid
       |from t group by company_id,applicant_name
       |),
       |ttt as(
       |select company_id,applicant_name,row_number()over(partition by company_id order by cnt desc,mxtime desc,mxid desc) rn
       |from tt
       |)
       |select * from ttt where rn=1
       |limit 10000
       |
       |""".stripMargin
  }
}
