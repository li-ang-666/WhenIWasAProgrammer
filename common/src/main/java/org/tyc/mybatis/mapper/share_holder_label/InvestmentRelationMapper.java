package org.tyc.mybatis.mapper.share_holder_label;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.tyc.entity.InvestmentRelation;

import java.util.List;

public interface InvestmentRelationMapper {

    /**
     * 根据investmentRelation中的CompanyId查询其所有的股东list
     *
     * @param investmentRelation 被投资公司
     * @return 所有一层的直接股东
     */
    @Select("select * FROM investment_relation WHERE company_id_invested = #{companyIdInvested}")
    public List<InvestmentRelation> getAllInvestmentRelationAsList(InvestmentRelation investmentRelation);


    @Select("select * FROM investment_relation WHERE company_id_invested = #{companyIdInvested} and investment_ratio >= 0.05")
    public List<InvestmentRelation> getAllInvestmentRelationAsListByGid(@Param("companyIdInvested") Long edCompanyId);

    @Select("select * FROM investment_relation WHERE company_id_invested = #{companyIdInvested} limit 1")
    public InvestmentRelation getAInvestmentRelationAsListByGid(@Param("companyIdInvested") Long edCompanyId);

    @Select("select * FROM investment_relation WHERE company_id_invested = #{companyId} and investment_ratio >= 0.05")
    public List<InvestmentRelation> getUpperCompanyByGid(@Param("companyId") Long companyId);

    @Select("select * FROM investment_relation WHERE shareholder_name_id = #{companyId} and shareholder_entity_type=1 and investment_ratio >= 0.05")
    public List<InvestmentRelation> getDownCompanyByGid(@Param("companyId") Long companyId);

    /**
     * 查询是否有符合穿透的股东.
     *
     * @return
     */
    @Select("select id FROM investment_relation WHERE company_id_invested = #{companyId} and investment_ratio >= 0.05 limit 1")
    Long getHaveAnyShareholders(@Param("companyId") Long companyId);
}
