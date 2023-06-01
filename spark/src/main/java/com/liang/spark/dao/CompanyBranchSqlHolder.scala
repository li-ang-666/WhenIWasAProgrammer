package com.liang.spark.dao

import com.liang.common.util.DateTimeUtils

class CompanyBranchSqlHolder {
  def queryTotalBranch(): String = {
    s"""
       |-- 总共有几个分支机构
       |
       |select t1.company_id,count(1)
       |from dwc.dwc_new_company_base_company_branch_df t1
       |where t1.company_id<>0
       |and t1.pt='${DateTimeUtils.getLastNDateTime(1, "yyyyMMdd")}'
       |group by t1.company_id
       |
       |""".stripMargin
  }

  def queryTotalCanceledBranch(): String = {
    s"""
       |-- 总共注销几个分支机构
       |
       |select t1.company_id,count(1)
       |from dwc.dwc_new_company_base_company_branch_df t1
       |join ods_company_base_company_index_df t2 on t1.branch_company_id = t2.company_id
       |where t1.pt='${DateTimeUtils.getLastNDateTime(1, "yyyyMMdd")}' and t2.pt='${DateTimeUtils.getLastNDateTime(1, "yyyyMMdd")}'
       |and t1.company_id<>0
       |and t2.dw_is_del=0
       |and (t2.company_registation_status like '%注销%' or t2.company_registation_status like '%吊销%')
       |group by t1.company_id
       |
       |""".stripMargin
  }

  def queryMostYear(): String = {
    s"""
       |-- 最频繁的年份
       |
       |with t as(
       |select t1.company_id,year(t2.establish_date) yr,count(1) cnt,max(t2.establish_date) mx
       |from dwc.dwc_new_company_base_company_branch_df t1
       |join ods_company_base_company_index_df t2 on t1.branch_company_id = t2.company_id
       |where t1.pt='${DateTimeUtils.getLastNDateTime(1, "yyyyMMdd")}' and t2.pt='${DateTimeUtils.getLastNDateTime(1, "yyyyMMdd")}'
       |and t1.company_id<>0
       |and t2.dw_is_del=0
       |and t2.establish_date is not null
       |group by t1.company_id,year(t2.establish_date)
       |),
       |tt as(
       |select company_id,yr,row_number()over(partition by company_id order by cnt desc,mx desc) rn
       |from t
       |)
       |select * from tt where rn=1
       |
       |""".stripMargin
  }

  def queryMostArea(): String = {
    s"""
       |-- 最集中的省/直辖市
       |
       |with t as(
       |select t1.company_id,t2.province_base_by_register_institute code,count(1) cnt,max(nvl(t2.establish_date,'1970-01-01')) mx
       |from dwc.dwc_new_company_base_company_branch_df t1
       |join ods_company_base_company_index_df t2 on t1.branch_company_id = t2.company_id
       |where t1.pt='${DateTimeUtils.getLastNDateTime(1, "yyyyMMdd")}' and t2.pt='${DateTimeUtils.getLastNDateTime(1, "yyyyMMdd")}'
       |and t1.company_id<>0
       |and t2.dw_is_del=0
       |and t2.province_base_by_register_institute is not null
       |and t2.province_base_by_register_institute<>''
       |group by t1.company_id,t2.province_base_by_register_institute
       |),
       |tt as(
       |select company_id,code,row_number()over(partition by company_id order by cnt desc,mx desc) rn
       |from t
       |),
       |final_left as(
       |select company_id,code from tt where rn=1
       |),
       |final_right as(
       |select distinct province_base code,province from ods_company_base_area_code_district_df
       |where pt = '${DateTimeUtils.getLastNDateTime(1, "yyyyMMdd")}'
       |)
       |select l.company_id,r.province
       |from final_left l join final_right r on l.code=r.code
       |
       |""".stripMargin
  }

}
