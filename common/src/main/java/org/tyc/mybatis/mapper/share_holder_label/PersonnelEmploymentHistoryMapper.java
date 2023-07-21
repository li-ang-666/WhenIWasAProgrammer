package org.tyc.mybatis.mapper.share_holder_label;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

public interface PersonnelEmploymentHistoryMapper {

    /**
     * 通过人员的nameId 和 公司id获取在公司的任职岗位list
     *
     * @param companyId   公司id
     * @param humanNameId 人员nameId
     * @return 岗位list
     */
    @Select("select  group_concat(distinct personnel_position) as position_list From personnel_employment_history \n" +
            "where  company_id = #{company_id} and personnel_name_id = #{personnel_name_id} \n" +
            "and is_deleted = 0 and is_history = 0 \n" +
            "group by company_id,personnel_name_id")
    String queryPositions(@Param("company_id") String companyId, @Param("personnel_name_id") String humanNameId);


    /**
     * 查询一个公司的所有的人员任职岗位
     *
     * @param companyId 公司id
     * @return 任职list
     */
    @Select("select personnel_name_id as human_name_id," +
            " group_concat(distinct personnel_position) as position_list," +
            " personnel_name human_name " +
            " from personnel_employment_history " +
            " where company_id = #{company_id} " +
            " and is_deleted = 0 and is_history = 0" +
            " group by company_id,personnel_name_id,personnel_name")
    List<Map<String, Object>> findAllPositionsByCompanyId(@Param("company_id") Long companyId);

}
