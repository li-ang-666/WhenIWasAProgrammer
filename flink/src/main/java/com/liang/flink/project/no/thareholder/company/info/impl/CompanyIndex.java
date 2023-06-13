package com.liang.flink.project.no.thareholder.company.info.impl;

import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.no.thareholder.company.info.dao.CompanyIndexDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>create table no_shareholder_company_info(
 * <p>  id bigint NOT NULL AUTO_INCREMENT COMMENT '主键',
 * <p>  `company_id_invested` bigint NOT NULL DEFAULT '0' COMMENT '被投资公司的gid',
 * <p>  `company_name_invested` varchar(255) NOT NULL DEFAULT '' COMMENT '被投资公司的name',
 * <p>  `company_type_invested` varchar(255) NOT NULL DEFAULT '' COMMENT '被投资公司的企业类型：不清洗枚举值，保留原来的类似 家庭经营',
 * <p>  `is_listed_company_invested` tinyint NOT NULL DEFAULT '0' COMMENT '被投资公司是否为上市公司 0-不是 1-是',
 * <p>  `is_foreign_branches_invested` tinyint NOT NULL DEFAULT '0' COMMENT '被投资公司是否是外国分支机构0-不是 1-是',
 * <p>  `listed_company_actual_controller_invested` text NOT NULL DEFAULT '' COMMENT '被投资公司如果是上市公司的话存储的是实际控制人，存为内链，逗号分割',
 * <p>  `is_partnership_company_invested` tinyint NOT NULL DEFAULT '0' COMMENT '被投资公司是否为合伙企业 0-不是 1-是',
 * <p>  `legal_rep_inlinks_invested` text NOT NULL DEFAULT '' COMMENT '被投资公司法定代表人，存为内链，逗号分割，如果是合伙企业存执行事务合伙人，如果是外国分支机构存负责人',
 * <p>  `company_uscc_prefix_code_two_invested` varchar(255) NOT NULL DEFAULT '' COMMENT '被投资公司统一社会信用代码前两位',
 * <p>  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
 * <p>  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录创建时间',
 * <p>  primary key (id)
 * <p>)DEFAULT CHARSET = utf8mb4 COMMENT = '没有股东的投资信息表';
 */
public class CompanyIndex extends AbstractDataUpdate<Map<String, Object>> {
    private final CompanyIndexDao dao = new CompanyIndexDao();

    @Override
    public List<Map<String, Object>> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String companyId = String.valueOf(columnMap.get("company_id"));
        String companyName = String.valueOf(columnMap.get("company_name"));
        String orgType = String.valueOf(columnMap.get("org_type"));
        String companyType = String.valueOf(columnMap.get("company_type"));
        String unifiedSocialCreditCode = String.valueOf(columnMap.get("unified_social_credit_code"));

        HashMap<String, Object> resultMap = new HashMap<>();
        resultMap.put("id", companyId);
        resultMap.put("company_id_invested", companyId);
        resultMap.put("company_name_invested", companyName);
        resultMap.put("company_type_invested", orgType);
        boolean isListedCompanyInvested = dao.queryIsListedCompanyInvested(companyId);
        resultMap.put("is_listed_company_invested", isListedCompanyInvested);
        resultMap.put("is_foreign_branches_invested", isForeignBranchesInvested(orgType));
        if (isListedCompanyInvested) {
            resultMap.put("listed_company_actual_controller_invested", dao.queryListedCompanyActualControllerInvested(companyId));
        }
        resultMap.put("is_partnership_company_invested", "11".equals(companyType));
        resultMap.put("legal_rep_inlinks_invested", dao.queryLegalRepInLinksInvested(companyId));
        resultMap.put("company_uscc_prefix_code_two_invested", unifiedSocialCreditCode.length() >= 2 ? unifiedSocialCreditCode.substring(0, 2) : "");
        return Collections.singletonList(resultMap);
    }

    private boolean isForeignBranchesInvested(String orgType) {
        return orgType.matches(".*?外国.*?分.*|.*?外国.*?驻.*|.*?外国.*?境内.*")
                && !orgType.contains("外国法人独资")
                && !orgType.contains("外国自然人独资")
                && !orgType.contains("台湾澳与外国投资者合资")
                && !orgType.contains("新闻")
                && !orgType.contains("旅游")
                && !orgType.contains("外国非法人");
    }
}
