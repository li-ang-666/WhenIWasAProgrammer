package com.liang.flink.dao

class RestrictedOutboundIndexSql {
  def queryMostApplicantSql(companyId: String): String = {
    s"""
       |-- 限制出境最多的申请人(限制出境,涉及申请人、被执行人(一般是公司)、限制出境对象(一般是具体的人))
       |
       |select inlink_execution_applicant, count(1)
       |from restricted_outbound_index
       |where company_id = ${companyId}
       |  and is_deleted = 0
       |  and inlink_execution_applicant is not null
       |  and inlink_execution_applicant <> ''
       |group by inlink_execution_applicant
       |order by count(1) desc, max(filing_time) desc, min(main_id) asc
       |
       |""".stripMargin
  }

  def queryMostRestrictedSql(companyId: String): String = {
    s"""
       |-- 页面上所有限制出境的记录里,被限制最多次的对象是
       |
       |select t2.restricted_name,count(1)
       |from restricted_outbound_index t1
       |         join restricted_outbound t2 on t1.main_id = t2.id
       |where t1.company_id = ${companyId}
       |  and t1.is_deleted = 0
       |  and t2.restricted_name is not null
       |  and t2.restricted_name <> ''
       |  and t2.restricted_name <> '其它'
       |  and t2.restricted_name <> '其他'
       |group by t2.restricted_name
       |order by count(1) desc, max(t1.filing_time) desc, min(main_id) asc
       |
       |""".stripMargin
  }
}
