package com.liang.spark.dao

class DataConcatSqlHolder {
  def queryMostApplicantSql(): String = {
    """
      |with t as(
      |    select company_id,if(applicant_inlink is not null and applicant_inlink <> '', applicant_inlink, applicant) fi, count(1) cnt
      |from ods_judicial_risk_restrict_consumption_split_index_df
      |where applicant <> ''
      |  and is_history = 0
      |  and company_id <> 0
      |group by company_id,if(applicant_inlink is not null and applicant_inlink <> '', applicant_inlink, applicant)
      |order by cnt desc
      |),tt as(
      |    select company_id,fi,row_number() over(partition by company_id order by cnt desc) rn from t
      |    )
      |select * from tt where rn = 1
      |limit 10000
      |""".stripMargin
  }
}
