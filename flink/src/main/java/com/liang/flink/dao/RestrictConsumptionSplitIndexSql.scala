package com.liang.flink.dao

class RestrictConsumptionSplitIndexSql {
  def queryMostApplicantSql(companyId: String, isHistory: Boolean): String = {
    s"""
       |-- 最多的申请人是(限制消费,涉及到申请人、被限制对象(阳性)、关联被限制对象(密接))
       |
       |select if(applicant_inlink is not null and applicant_inlink <> '', applicant_inlink, applicant) fi, count(1)
       |from restrict_consumption_split_index
       |where applicant <> ''
       |  and company_id = ${companyId}
       |  and is_history = ${if (isHistory) 1 else 0}
       |group by fi
       |order by count(1) desc, max(case_create_time) desc, max(id) desc
       |
       |""".stripMargin
  }

  def queryMostRelatedRestrictedSql(restrictedId: String, isHistory: Boolean): String = {
    s"""
       |-- 作为被限制消费对象(阳性),最多的被关联限制消费对象(密接)是
       |
       |select restrict_consumption_inlink, count(1)
       |from restrict_consumption_split_index
       |where is_history =  ${if (isHistory) 1 else 0}
       |  and company_info_company_id = ${restrictedId}
       |  and company_id = ${restrictedId}
       |  and restrict_consumption_inlink <> ''
       |group by restrict_consumption_inlink
       |order by count(1) desc, max(case_create_time) desc, max(id) desc
       |
       |""".stripMargin
  }

  def queryMostRestrictedSql(relatedRestrictedId: String, isHistory: Boolean): String = {
    s"""
       |-- 作为被关联限制消费对象(密接),最多的被限制消费对象(阳性)是
       |
       |select company_info_inlink, count(1)
       |from restrict_consumption_split_index
       |where is_history =  ${if (isHistory) 1 else 0}
       |  and restrict_consumption_company_id = ${relatedRestrictedId}
       |  and company_id = ${relatedRestrictedId}
       |  and company_info_inlink <> ''
       |group by company_info_inlink
       |order by count(1) desc, max(case_create_time) desc, max(id) desc
       |
       |""".stripMargin
  }
}
