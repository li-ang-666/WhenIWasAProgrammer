package com.liang.flink.project.evaluation.institution.candidate.impl

import com.liang.flink.project.evaluation.institution.candidate.EvaluationInstitutionCandidateService

object SqlHolder {
  def getDeleteSql(caseNumber: String): String = {
    s"""
       |delete from ${EvaluationInstitutionCandidateService.TABLE.replace("middle", "details")}
       |where enforcement_case_number = '${caseNumber}'
       |""".stripMargin
  }

  def main(args: Array[String]): Unit = {
    println(getInsertSql("aaa"))
  }

  def getInsertSql(caseNumber: String): String = {
    s"""
       |insert into ${EvaluationInstitutionCandidateService.TABLE.replace("middle", "details")}
       |select
       |  `tyc_unique_entity_id_subject_to_enforcement`,
       |  `entity_name_valid_subject_to_enforcement`,
       |  `entity_type_id_subject_to_enforcement`,
       |  `tyc_unique_entity_id_candidate_evaluation_institution`,
       |  `entity_name_valid_selected_evaluation_institution`,
       |  `entity_type_id_candidate_evaluation_institution`,
       |  `enforcement_case_number`,
       |  `enforcement_case_number_original`,
       |  `enforcement_case_type`,
       |  `enforcement_object_evaluation_court_name`,
       |  `enforcement_object_asset_type`,
       |  `enforcement_object_name`,
       |  `lottery_date_to_candidate_evaluation_institution`,
       |  `is_evaluation_institution_candidate`,
       |  `delete_status`
       |from ${EvaluationInstitutionCandidateService.TABLE}
       |where enforcement_case_number = (
       |  select enforcement_case_number
       |  from ${EvaluationInstitutionCandidateService.TABLE}
       |  where enforcement_case_number = '${caseNumber}'
       |  group by enforcement_case_number
       |  having min(entity_type_id_subject_to_enforcement) = 1 and sum(is_state_organs) = 0
       |  limit 1
       |)
       |""".stripMargin
  }
}
