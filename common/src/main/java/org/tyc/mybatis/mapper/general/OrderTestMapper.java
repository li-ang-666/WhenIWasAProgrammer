package org.tyc.mybatis.mapper.general;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.io.Serializable;

/**
 * @author wangshiwei
 * @date 2022/3/21 6:04 下午
 * @description
 **/
public interface OrderTestMapper extends Serializable {
    @Select("SELECT order_id FROM order_info WHERE user_id=#{userId} and status = 1 and type in (45, 46, 47, 50, 60, 70, 55, 56, 57) and pay_date <= #{payDate} limit 1")
    String queryFirstOrder(@Param("userId") Long userId, @Param("payDate") String payDate);


}
