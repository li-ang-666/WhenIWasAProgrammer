package org.tyc.mybatis.mapper.share_holder_label;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.tyc.entity.InvestmentRelation;

/**
 * @author wangshiwei
 * @date 2023/6/15 17:29
 * @description
 **/
public interface NoShareholderCompanyInfoMapper {

    @Select("select * FROM no_shareholder_company_info WHERE company_id_invested = #{companyIdInvested} limit 1")
    public InvestmentRelation getAInvestmentRelationAsListByGid(@Param("companyIdInvested") Long edCompanyId);

}
