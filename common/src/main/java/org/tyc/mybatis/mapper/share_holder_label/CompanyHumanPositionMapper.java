package org.tyc.mybatis.mapper.share_holder_label;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * @author wangshiwei
 * @date 2023/5/9 16:44
 * @description
 **/
public interface CompanyHumanPositionMapper {
    @Select("select position_list from company_human_position where company_id = #{company_id} and human_name_id = #{human_name_id} limit 1")
    String queryPositions(@Param("company_id") String companyId, @Param("human_name_id") String humanNameId);

    @Select("select human_name_id,position_list,human_name from company_human_position where company_id = #{company_id} ")
    List<Map<String, Object>> findAllPositionsByCompanyId(@Param("company_id") Long companyId);
}
