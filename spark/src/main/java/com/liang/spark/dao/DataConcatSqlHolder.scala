package com.liang.spark.dao

class DataConcatSqlHolder {
  def restrictedConsumptionMostApplicantSql(): String = {
    """
      |-- 限制消费令最多的申请人
      |
      |with t as(
      |select t1.company_id,t1.case_create_time,t1.id,tmp_tb.tmp_col applicant_name
      |from ods_judicial_risk_restrict_consumption_split_index_df t1
      |lateral view explode(split(if(applicant_inlink is not null and applicant_inlink <> '', applicant_inlink, applicant),';')) tmp_tb as tmp_col
      |where t1.company_id<>0
      |and t1.is_history=0
      |and t1.applicant is not null and t1.applicant<>''
      |and t1.applicant<>'其他'
      |and t1.applicant<>'其它'
      |),
      |tt as(
      |select company_id,applicant_name,count(1) cnt,max(case_create_time) mxtime,max(id) mxid
      |from t group by company_id,applicant_name
      |),
      |ttt as(
      |select company_id,applicant_name,row_number()over(partition by company_id order by cnt desc,mxtime desc,mxid desc) rn
      |from tt
      |)
      |select * from ttt where rn=1 limit 111
      |
      |""".stripMargin
  }

  def restrictedConsumptionMostRelatedRestrictedSql(): String = {
    """
      |-- 作为被限制消费对象(阳性),最多的被关联限制消费对象(密接)是
      |
      |with t as(
      |select t1.company_info_company_id,t1.case_create_time,t1.id,tmp_tb.tmp_col inlink
      |from ods_judicial_risk_restrict_consumption_split_index_df t1
      |lateral view explode(split(t1.restrict_consumption_inlink,';')) tmp_tb as tmp_col
      |where t1.is_history=0
      |and t1.company_info_company_id<>0
      |and t1.restrict_consumption_inlink is not null and t1.restrict_consumption_inlink <>''
      |),
      |tt as(
      |select company_info_company_id,inlink,count(1) cnt,max(case_create_time) mxtime,max(id) mxid
      |from t group by company_info_company_id,inlink
      |),
      |ttt as(
      |select company_info_company_id,inlink,row_number()over(partition by company_info_company_id order by cnt desc,mxtime desc,mxid desc) rn
      |from tt
      |)
      |select * from ttt where rn=1 limit 11111
      |
      |""".stripMargin
  }

  def restrictedConsumptionMostRestrictedSql(): String = {
    """
      |-- 作为被关联限制消费对象(密接),最多的被限制消费对象(阳性)是
      |
      |with t as(
      |select t1.restrict_consumption_company_id,t1.case_create_time,t1.id,tmp_tb.tmp_col inlink
      |from ods_judicial_risk_restrict_consumption_split_index_df t1
      |lateral view explode(split(t1.company_info_inlink,';')) tmp_tb as tmp_col
      |where t1.is_history=0
      |and t1.restrict_consumption_company_id<>0
      |and t1.company_info_inlink is not null and t1.company_info_inlink <>''
      |),
      |tt as(
      |select restrict_consumption_company_id,inlink,count(1) cnt,max(case_create_time) mxtime,max(id) mxid
      |from t group by restrict_consumption_company_id,inlink
      |),
      |ttt as(
      |select restrict_consumption_company_id,inlink,row_number()over(partition by restrict_consumption_company_id order by cnt desc,mxtime desc,mxid desc) rn
      |from tt
      |)
      |select * from ttt where rn=1 limit 11111
      |
      |""".stripMargin
  }
}
