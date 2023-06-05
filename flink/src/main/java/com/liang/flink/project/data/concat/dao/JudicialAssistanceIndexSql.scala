package com.liang.flink.project.data.concat.dao

class JudicialAssistanceIndexSql {
  def queryTotalFrozenEquitySql(CompanyIdOrEnforcedTargetId: String, isHistory: Boolean): String = {
    s"""
       |-- 作为被执行人(公司)或者被执行企业,有多少股权被冻结(股权冻结,涉及股权持有者、以及股权所属公司)
       |
       |select sum(t2.enforced_equity_value)
       |from judicial_assistance_index t1
       |         join judicial_assistance_info t2 on t1.main_id = t2.id
       |where (
       |            t1.company_id = ${CompanyIdOrEnforcedTargetId}
       |        or
       |            (t1.enforced_target_id = ${CompanyIdOrEnforcedTargetId} and t1.enforced_target_type = 1)
       |    )
       |  -- and enforced_equity_value <> ''
       |  and t1.judicial_assistance_type='股权冻结'
       |  and (t2.enforced_equity_value like '%万元%' or t2.enforced_equity_value like '%万%人民币%' or t2.enforced_equity_value like '%人民币%万%')
       |  and t1.is_deleted = ${if (isHistory) 1 else 0}
       |
       |""".stripMargin
  }

  def queryMostEquityFrozenCompanySql(enforcedTargetId: String, isHistory: Boolean): String = {
    s"""
       |-- 作为被执行人(公司),股权冻结最多的企业是
       |
       |select t1.company_id
       |from judicial_assistance_index t1
       |         join judicial_assistance_info t2 on t1.main_id = t2.id
       |where t1.enforced_target_id = ${enforcedTargetId}
       |  and t1.enforced_target_type = 1
       |  and t1.is_deleted = ${if (isHistory) 1 else 0}
       |  and t1.company_id <> 0
       |  -- and enforced_equity_value <> ''
       |  and t1.judicial_assistance_type='股权冻结'
       |group by t1.company_id
       |order by count(1) desc,max(t1.judicial_assistance_publish_date) desc,max(t1.main_id) desc
       |limit 1
       |
       |""".stripMargin
  }

  def queryMostEquityFrozenEnforcedTargetSql(companyId: String, isHistory: Boolean): String = {
    s"""
       |-- 作为被执行企业,股权冻结最多的被执行人是
       |
       |select t2.enforced_target_type, t2.enforced_target_id, t2.enforced_target_name
       |from judicial_assistance_index t1
       |         join judicial_assistance_info t2 on t1.main_id = t2.id
       |where t1.company_id = ${companyId}
       |  and t1.is_deleted = ${if (isHistory) 1 else 0}
       |  and t2.enforced_target_name <> ''
       |  and t2.enforced_target_name <> '其他'
       |  and t2.enforced_target_name <> '其它'
       |  -- and t2.enforced_equity_value <> ''
       |  and t1.judicial_assistance_type='股权冻结'
       |group by t2.enforced_target_type, t2.enforced_target_id, t2.enforced_target_name
       |order by count(1) desc,max(t1.judicial_assistance_publish_date) desc,max(t1.main_id) desc
       |limit 1
       |
       |""".stripMargin
  }
}
