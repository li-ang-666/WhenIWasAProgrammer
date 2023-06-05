package com.liang.flink.project.data.concat.dao

class EquityPledgeDetailSql {
  def totalEquitySql(companyId: String): String = {
    s"""
       |-- 某个公司总共质押了多少份股票
       |
       |select sum(pledged_shares_cnt * 10000)
       |from equity_pledge_detail
       |where company_id = ${companyId}
       |  and is_deleted = 0
       |  -- and pledged_shares_cnt > 0
       |
       |""".stripMargin
  }

  def maxTargetCompanySql(companyId: String): String = {
    s"""
       |-- 作为质押人,质押股权最多的标的公司
       |
       |select '1', company_id, company_name
       |from equity_pledge_detail
       |where pledgor_id = ${companyId}
       |  and company_id = ${companyId}
       |  and is_deleted = 0
       |  and company_name is not null
       |  and company_name <> ''
       |  and company_name <> '其他'
       |  and company_name <> '其它'
       |  -- and pledged_shares_cnt > 0
       |group by company_id, company_name
       |order by count(1) desc,max(equity_pledge_announcement_date) desc
       |limit 1
       |
       |""".stripMargin
  }

  def maxPledgorSql(companyId: String): String = {
    s"""
       |-- 作为标的公司,质押股票最多的质押人
       |
       |select if(pledgor_id is not null and pledgor_id <> 0, '1', '2'),
       |       if(pledgor_id is not null and pledgor_id <> 0, pledgor_id, null),
       |       pledgor_name
       |from equity_pledge_detail
       |where company_id = ${companyId}
       |  and is_deleted = 0
       |  and pledgor_name <> ''
       |  and pledgor_name <> '其它'
       |  and pledgor_name <> '其他'
       |  -- and pledged_shares_cnt > 0
       |group by pledgor_id, pledgor_name
       |order by count(1) desc,max(equity_pledge_announcement_date) desc
       |limit 1
       |
       |""".stripMargin
  }
}
