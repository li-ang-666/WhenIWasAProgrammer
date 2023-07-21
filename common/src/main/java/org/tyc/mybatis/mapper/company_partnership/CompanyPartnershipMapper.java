package org.tyc.mybatis.mapper.company_partnership;

import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * @author: 邵静虎
 * @Time: 2023/5/12 10:04
 */
public interface CompanyPartnershipMapper {
    @Select("select executive_gid,executive_type-1 executive_type from company_partnership_info where company_id=\n" +
            "(select company_id from company_graph where graph_id=#{companyId} and deleted=0 )\n" +
            "and deleted=0 ")
    List<Map<String, Object>> findPartnershipByCompanyId(Long companyId);

    @Select("select * from company\n" +
            "where id=\n" +
            "(select company_id from company_graph where graph_id=#{companyId} and deleted=0 ) limit 1")
    Map<String, Object> findCompanyByCompanyId(Long companyId);

    @Select("select graph_id from company_graph where company_id=#{cid} and deleted=0 limit 1")
    Long getGidByCid(Long cid);

    @Select("select name from company\n" +
            "where id in (\n" +
            "select company_id from company_graph where graph_id=#{companyId}\n" +
            ")")
    String findCompanyNameByCompanyId(Long companyId);

    @Select("select name from human\n" +
            "where id in  (\n" +
            "select human_id from human_graph where graph_id=#{gid})")
    String findHumanNameByGid(Long gid);
}
