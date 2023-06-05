package com.liang.flink.project.data.concat.dao

class EquityPledgeReinvestSql {
  def totalEquitySql(companyId: String, isHistory: Boolean): String = {
    s"""
       |-- 作为出质人(公司)or标的公司,出质股权数量
       |
       |select sum(equity_pledge_value)
       |from equity_pledge_reinvest
       |where (
       |        (pledgor_entity_id = ${companyId} and pledgor_type = 1)
       |        or
       |        company_id = ${companyId}
       |    )
       |  and is_deleted = ${if (isHistory) 1 else 0}
       |  and pledgor_type in (1, 2)
       |  and pledgee_type in (1, 2)
       |   -- and equity_pledge_state in ('正常', '有效')
       |  -- and equity_pledge_value != ''
       |  and (equity_pledge_value like '%万元%' or equity_pledge_value like '%万%人民币%' or equity_pledge_value like '%人民币%万%')
       |
       |""".stripMargin
  }

  def maxTargetCompanySql(companyId: String, isHistory: Boolean): String = {
    s"""
       |-- 作为出质人(公司),最多的标的公司
       |
       |select '1', t1.company_id, '占位符'
       |from equity_pledge_reinvest t1
       |          -- left join equity_pledge_reinvest_index t2 on t1.id = t2.main_id and t2.equity_pledgee_type = 2
       |where t1.pledgor_entity_id = ${companyId}
       |  and t1.pledgor_type = 1
       |  and t1.is_deleted = ${if (isHistory) 1 else 0}
       |  and t1.pledgee_type in (1, 2)
       |   -- and t1.equity_pledge_state in ('正常', '有效')
       |  -- and t1.equity_pledge_value != ''
       |   -- and t2.equity_pledge_target_company_name <> ''
       |   -- and t2.equity_pledge_target_company_name <> '其他'
       |   -- and t2.equity_pledge_target_company_name <> '其它'
       |group by t1.company_id, '占位符'
       |order by count(1) desc,max(t1.equity_pledge_register_date) desc
       |limit 1
       |
       |""".stripMargin
  }

  def maxPledgorSql(companyId: String, isHistory: Boolean): String = {
    s"""
       |-- 作为标的公司,最多的出质人
       |
       |select pledgor_type, pledgor_entity_id, pledgor_name
       |from equity_pledge_reinvest
       |where company_id = ${companyId}
       |  and is_deleted = ${if (isHistory) 1 else 0}
       |  and pledgor_type in (1, 2)
       |  and pledgee_type in (1, 2)
       |   -- and equity_pledge_state in ('正常', '有效')
       |  -- and equity_pledge_value != ''
       |  and pledgor_name != ''
       |  and pledgor_name != '其他'
       |  and pledgor_name != '其它'
       |group by pledgor_type, pledgor_entity_id, pledgor_name
       |order by count(1) desc,max(equity_pledge_register_date) desc
       |limit 1
       |
       |""".stripMargin
  }
}
