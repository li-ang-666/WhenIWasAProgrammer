package org.tyc.mybatis.mapper.company_base;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;


public interface TycEntityMainReferenceMapper {
    @Select("select entity_name_valid from tyc_entity_main_reference where tyc_unique_entity_id=#{tyc_unique_entity_id} limit 1")
    public String getEntityNameValidByTycUniqueEntityId(@Param("tyc_unique_entity_id") String tyc_unique_entity_id);
}
