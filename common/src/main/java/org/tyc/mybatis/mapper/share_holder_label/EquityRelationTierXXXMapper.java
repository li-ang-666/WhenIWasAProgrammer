package org.tyc.mybatis.mapper.share_holder_label;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * @author wangshiwei
 * @date 2023/5/11 13:49
 * @description
 **/
public interface EquityRelationTierXXXMapper {
    /**
     * 每年一张表 @qiaozhiwei
     * 获取股东向上的层数
     *
     * @param companyId
     * @return
     */
    @Select("select shareholder_tier_cnt from equity_relation_tier_${year_value} where tyc_unique_entity_id = #{company_id}  limit 1")
    Long getShareholderTierCnt(@Param("year_value") String yearValue, @Param("company_id") String companyId);
}
