package org.tyc.mybatis.mapper.general;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * @author wangshiwei
 * @date 2022/3/21 6:04 下午
 * @description
 **/
public interface TestMapper {
    @Select("SELECT  user_id FROM order_info WHERE id=#{id}")
    String queryTest(@Param("id") Long id);

    @Select("SELECT  company_name FROM administration_punish_creditchina WHERE id=#{id}")
    String queryTest2(@Param("id") Long id);

}
