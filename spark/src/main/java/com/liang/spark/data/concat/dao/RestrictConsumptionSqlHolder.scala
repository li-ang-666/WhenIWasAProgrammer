package com.liang.spark.data.concat.dao

import com.liang.common.util.DateTimeUtils

class RestrictConsumptionSqlHolder {
  def queryMostApplicant(isHistory: Boolean): String = {
    s"""
       |-- 最多的限制消费令申请人
       |
       |with t as(
       |select t1.company_id,t1.case_create_time,t1.id,tmp_tb.tmp_col applicant_name
       |from ods_judicial_risk_restrict_consumption_split_index_df t1
       |lateral view explode(split(if(applicant_inlink is not null and applicant_inlink <> '', applicant_inlink, applicant),';')) tmp_tb as tmp_col
       |where t1.company_id<>0 and t1.pt='${DateTimeUtils.getLastNDateTime(1, "yyyyMMdd")}'
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
       |
       |""".stripMargin
  }

  def queryMostRelatedRestricted(isHistory: Boolean): String = {
    s"""
       |-- 作为被限制消费对象(阳性),最多的被关联限制消费对象(密接)是
       |
       |with t as(
       |select t1.company_info_company_id,t1.case_create_time,t1.id,tmp_tb.tmp_col inlink
       |from ods_judicial_risk_restrict_consumption_split_index_df t1
       |lateral view explode(split(t1.restrict_consumption_inlink,';')) tmp_tb as tmp_col
       |where t1.is_history=${if (isHistory) 1 else 0} and t1.pt='${DateTimeUtils.getLastNDateTime(1, "yyyyMMdd")}'
       |and t1.company_info_company_id<>0
       |and t1.company_info_company_id = t1.company_id
       |and t1.restrict_consumption_inlink is not null and t1.restrict_consumption_inlink <>''
       |and t1.dw_is_del =0
       |),
       |tt as(
       |select company_info_company_id,inlink,count(1) cnt,max(case_create_time) mxtime,max(id) mxid
       |from t group by company_info_company_id,inlink
       |),
       |ttt as(
       |select company_info_company_id,inlink,row_number()over(partition by company_info_company_id order by cnt desc,mxtime desc,mxid desc) rn
       |from tt
       |)
       |select * from ttt where rn=1
       |
       |""".stripMargin
  }

  def queryMostRestrivted(isHistory: Boolean): String = {
    s"""
       |-- 作为被关联限制消费对象(密接),最多的被限制消费对象(阳性)是
       |
       |with t as(
       |select t1.restrict_consumption_company_id,t1.case_create_time,t1.id,tmp_tb.tmp_col inlink
       |from ods_judicial_risk_restrict_consumption_split_index_df t1
       |lateral view explode(split(t1.company_info_inlink,';')) tmp_tb as tmp_col
       |where t1.is_history=${if (isHistory) 1 else 0} and t1.pt='${DateTimeUtils.getLastNDateTime(1, "yyyyMMdd")}'
       |and t1.restrict_consumption_company_id<>0
       |and t1.restrict_consumption_company_id=t1.company_id
       |and t1.company_info_inlink is not null and t1.company_info_inlink <>''
       |and t1.dw_is_del =0
       |),
       |tt as(
       |select restrict_consumption_company_id,inlink,count(1) cnt,max(case_create_time) mxtime,max(id) mxid
       |from t group by restrict_consumption_company_id,inlink
       |),
       |ttt as(
       |select restrict_consumption_company_id,inlink,row_number()over(partition by restrict_consumption_company_id order by cnt desc,mxtime desc,mxid desc) rn
       |from tt
       |)
       |select * from ttt where rn=1
       |
       |""".stripMargin
  }
}
