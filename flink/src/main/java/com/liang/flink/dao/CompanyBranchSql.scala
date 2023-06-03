package com.liang.flink.dao

class CompanyBranchSql {
  def queryTotalBranchSql(companyId: String): String = {
    s"""
       |-- 总共几个分支机构
       |
       |select count(1)
       |from company_index
       |where company_id in (select branch_company_id from company_branch where company_id = ${companyId} and is_deleted = 0)
       |and company_id <> 0
       |
       |""".stripMargin
  }

  def queryTotalCanceledBranchSql(companyId: String): String = {
    s"""
       |-- 总共几个注销吊销分支机构
       |
       |select count(1)
       |from company_index
       |where company_id in (select branch_company_id from company_branch where company_id = ${companyId} and is_deleted = 0)
       |and (company_registation_status like '%注销%' or company_registation_status like '%吊销%')
       |and company_id <> 0
       |
       |""".stripMargin
  }

  def queryMostYearSql(companyId: String): String = {
    s"""
       |-- 成立最频繁的年份
       |
       |select year(establish_date)
       |from company_index
       |where company_id in (select branch_company_id from company_branch where company_id = $companyId and is_deleted = 0)
       |  and establish_date is not null
       |  and company_id <> 0
       |group by year(establish_date)
       |order by count(1) desc, max(establish_date) desc
       |limit 1;
       |
       |""".stripMargin
  }

  def queryMostAreaSql(companyId: String): String = {
    s"""
       |-- 成立最多的地区
       |
       |select province
       |from area_code_district
       |where province_base =
       |      (select province_base_by_register_institute
       |       from company_index
       |       where company_id in (select branch_company_id from company_branch where company_id = ${companyId} and is_deleted = 0)
       |         and company_id <> 0
       |         and establish_date is not null
       |         and province_base_by_register_institute <> ''
       |       group by province_base_by_register_institute
       |       order by count(1) desc, max(ifnull(establish_date, '1970-01-01')) desc
       |       limit 1)
       |limit 1
       |
       |""".stripMargin
  }
}
