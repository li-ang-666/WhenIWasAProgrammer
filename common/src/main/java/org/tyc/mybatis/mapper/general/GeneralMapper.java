package org.tyc.mybatis.mapper.general;

import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * @author wangshiwei
 * @date 2023/2/1 10:24
 * @description
 **/
public interface GeneralMapper {

    @Select("${sql}")
    String selectOne(String sql);

    @Select("${sql}")
    List<Map<String, String>> selectMore(String sql);

}
