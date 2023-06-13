package com.liang.flink.project.no.thareholder.company.info.dao

import com.liang.common.service.database.template.JdbcTemplate
import org.apache.commons.lang3.StringUtils

class CompanyIndexDao {
  private final val prism1: JdbcTemplate = new JdbcTemplate("prism1")
  private final val listed: JdbcTemplate = new JdbcTemplate("listed")
  private final val companyBase: JdbcTemplate = new JdbcTemplate("companyBase")

  def queryIsListedCompanyInvested(companyId: String): Boolean = {
    val sql: String =
      s"""
         |select 1
         |from company_bond_plates t1
         |         join company_graph t2
         |              on t1.company_id = t2.company_id
         |where t1.deleted = 0
         |  and t1.listing_status not in ('暂停上市', 'IPO终止', '退市整理', '终止上市')
         |  and t2.deleted = 0
         |  and t2.graph_id = $companyId
         |""".stripMargin
    val res: String = prism1.queryForObject(sql, rs => rs.getString(1))
    res != null
  }

  def queryListedCompanyActualControllerInvested(companyId: String): String = {
    val sql: String =
      s"""
         |select group_concat(
         |               case
         |                   when controller_type = 0 then concat(controller_name, ':', controller_gid, '-', graph_id, ':',
         |                                                        controller_pid, ':human')
         |                   when controller_type = 1 then concat(controller_name, ':',
         |                                                        if(controller_gid = 0, '', controller_gid), ':company')
         |                   else concat(controller_name, ':', if(controller_gid = 0, '', controller_gid), ':other') end
         |           ) as listed_company_actual_controller
         |from stock_actual_controller
         |where is_deleted = 0
         |  and graph_id = $companyId
         |group by graph_id
         |""".stripMargin
    val res: String = listed.queryForObject(sql, rs => rs.getString(1))
    if (StringUtils.isNotBlank(res)) res else ""
  }

  def queryLegalRepInLinksInvested(CompanyId: String): String = {
    val sql: String =
      s"""
         |select group_concat(
         |               case
         |                   when legal_rep_type = 2 then concat(ifnull(legal_rep_name, ''), ':', if(legal_rep_name_id != 0,
         |                                                                                           concat(legal_rep_name_id, '-', company_id),
         |                                                                                           ''), ':',
         |                                                       ifnull(legal_rep_human_id, ''), ':human')
         |                   when legal_rep_type = 1 then concat(ifnull(legal_rep_name, ''), ':',
         |                                                       if(legal_rep_name_id != 0, legal_rep_name_id, ''), ':company')
         |                   else concat(ifnull(legal_rep_name, ''), ':', if(legal_rep_name_id != 0, legal_rep_name_id, ''),
         |                               ':other') end
         |           ) as legal_rep_inlinks
         |from company_legal_person
         |where company_id = $CompanyId
         |group by company_id
         |""".stripMargin
    val res: String = companyBase.queryForObject(sql, rs => rs.getString(1))
    if (StringUtils.isNotBlank(res)) res else ""
  }
}
