package org.tyc.entity;


import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;


/**
 * 股权关系中间表
 */
@Data
public class InvestmentRelation implements Cloneable, Serializable {

    public Set<String> companyItemPositionsSet;
    /**
     * 1-controller, 0-not controller
     */
    public Integer isController;
    /**
     * 1-ultimate, 0-not ultimate
     */
    public Integer isUltimate;
    /**
     * 1-is big_shareholder, 0-not big_shareholder
     */
    public Integer isBigShareholder;
    /**
     * 1-is controlling_shareholder, 0-not controlling_shareholder
     */
    public Integer isControllingShareholder;
    /**
     * company->shareholder
     */
    public Path path;
    /**
     * 路径是不是终点路径
     */
    public Integer end;
    /**
     * 主键
     */
    private Long id;
    /**
     * 股东类型，1代表公司，2代表人
     */
    private Integer shareholderEntityType;
    /**
     * 投资公司的实体内链
     */
    private String companyEntityInlink;
    /**
     * 投资公司的企业类型：不清洗枚举值，保留原来的类似 家庭经营
     */
    private String companyType;
    /**
     * 投资公司是否为上市公司 0-不是 1-是
     */
    private Integer isListedCompany;
    /**
     * 投资公司是否是外国分支机构 0-不是 1-是
     */
    private Integer isForeignBranches;
    /**
     * 投资公司如果是上市公司的话存储的是实际控制人，存为内链，逗号分割
     */
    private String listedCompanyActualController;
    /**
     * 投资公司是否为合伙企业 0-不是 1-是
     */
    private Integer isPartnershipCompany;
    /**
     * 投资公司法定代表人，存为内链，逗号分割，如果是合伙企业存执行事务合伙人，如果是外国分支机构存负责人
     */
    private String legalRepInlinks;
    /**
     * 投资公司统一社会信用代码前两位
     */
    private String companyUsccPrefixCodeTwo;
    /**
     * 被投资公司的gid
     */
    private Long companyIdInvested;
    /**
     * 被投资公司的name
     */
    private String companyNameInvested;
    /**
     * 被投资公司的企业类型：不清洗枚举值，保留原来的类似 家庭经营
     */
    private String companyTypeInvested;
    /**
     * 被投资公司是否为上市公司 0-不是 1-是
     */
    private Integer isListedCompanyInvested;
    /**
     * 被投资公司是否是外国分支机构0-不是 1-是
     */
    private Integer isForeignBranchesInvested;
    /**
     * 被投资公司如果是上市公司的话存储的是实际控制人，存为内链，逗号分割
     */
    private String listedCompanyActualControllerInvested;
    /**
     * 被投资公司是否为合伙企业 0-不是 1-是
     */
    private Integer isPartnershipCompanyInvested;
    /**
     * 被投资公司法定代表人，存为内链，逗号分割，如果是合伙企业存执行事务合伙人，如果是外国分支机构存负责人
     */
    private String legalRepInlinksInvested;
    /**
     * 被投资公司统一社会信用代码前两位
     */
    private String companyUsccPrefixCodeTwoInvested;

    /**
     * 判定股东比例是不是大于指定值
     *
     * @param rationNum 比例值
     */
//    public boolean ifInvestmentRatioGreaterThan(BigDecimal rationNum) {
//        return investmentRatio.compareTo(rationNum) > 0;
//    }
    /**
     * 股东在当前公司的任职岗位-可以多个
     */
    private String shareholderCompanyPositionListClean;
    /**
     * 股东的出资比例
     */
    private BigDecimal investmentRatio;
    /**
     * 股东id，人hgid，公司gid, 其他id
     */
    private Long shareholderNameId;
    /**
     * 记录创建时间
     */
    private Date createTime;
    /**
     * 记录创建时间
     */
    private Date updateTime;

    /**
     * 无参构造
     */
    public InvestmentRelation() {
    }

    /**
     * 拼装岗位set
     *
     * @param shareholderCompanyPositionListClean string
     */
    public void setShareholderCompanyPositionListClean(String shareholderCompanyPositionListClean) {
        this.shareholderCompanyPositionListClean = shareholderCompanyPositionListClean;
        this.companyItemPositionsSet = new HashSet<>();
        if (StringUtils.isNotBlank(shareholderCompanyPositionListClean)) {
            Arrays.stream(shareholderCompanyPositionListClean.split(","))
                    .filter(StringUtils::isNotBlank)
                    .map(String::trim)
                    .forEach(this.companyItemPositionsSet::add);
        }

    }

    /**
     * 判定股东比例是不是大于等于指定值
     *
     * @param rationNum 比例值
     */
    public boolean ifInvestmentRatioGreaterEqualThan(BigDecimal rationNum) {
        return investmentRatio.compareTo(rationNum) >= 0;
    }

    /**
     * 判定股东是不是处于001名单
     */
    public boolean ifInvestorIn001() {
        return companyUsccPrefixCodeTwo.equals("11");
    }

    /**
     * 判定股东是否是自然人
     */
    public boolean ifInvestorIsPerson() {
        return Objects.equals(shareholderEntityType, EntityTypeEnum.PERSON);
    }

    /**
     * 判定股东是否不是自然人
     */
    public boolean ifNotInvestorIsPerson() {
        return !ifInvestorIsPerson();
    }

    /**
     * 判定股东是否是公司实体
     */
    public boolean ifInvestorIsCompany() {
        return Objects.equals(shareholderEntityType, EntityTypeEnum.COMPANY);
    }

    /**
     * 给定岗位，判定是不是股东的岗位
     *
     * @param post 岗位
     * @return 是否
     */
    public boolean ifCompanyItemPositions(String... post) {
        return Arrays.stream(post).anyMatch(companyItemPositionsSet::contains);
    }

    /**
     * 判定投资比例是否小于5%
     */
    public boolean ifShareholderPercentLessThan005() {
        return investmentRatio.compareTo(DigitEnum.RATION005) < 0;
    }

    /**
     * 获取路径的key
     */
    public String getKey(Long RootCompanyId) {
        String shareholderId = "";
        if (Objects.equals(shareholderEntityType, EntityTypeEnum.COMPANY)) { // 上海同兴袜厂:608822228:company
            shareholderId = companyEntityInlink.split(":")[1];
        } else if (Objects.equals(shareholderEntityType, EntityTypeEnum.PERSON)) { // 崔珊珊:1905070872-22822:U004AMH000T0444SH:human
            shareholderId = companyEntityInlink.split(":")[2];
            ;
        }
        return String.format("%s#%s#%s", RootCompanyId, shareholderEntityType, shareholderId);
    }


    /**
     * clone
     */
    @Override
    public InvestmentRelation clone() {
        try {
            InvestmentRelation cloned = (InvestmentRelation) super.clone();
            // 对于不可变对象（如String、Integer、Long、BigDecimal、Date等），直接复制引用即可
            // 对于可变对象（如Path类，如果它实现了深拷贝），我们需要调用clone()方法进行深拷贝
            cloned.path = path;
            return cloned;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone InvestmentRelation", e);
        }
    }


    /**
     * 检测路径上成环
     *
     * @return true or false
     */
    public boolean doCyclicReferenceDetected() {
        return path.getUsageCountForInvestmentRelation(this) >= 2;
    }
}

