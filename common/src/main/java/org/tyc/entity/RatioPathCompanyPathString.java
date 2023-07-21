package org.tyc.entity;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Objects;

/**
 * 路径关系中间表
 * 每个相关路径存一行
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RatioPathCompanyPathString implements Serializable {
    /**
     * 主键id
     */
    private Long id;

    /**
     * 公司id
     */
    private Long companyId;

    /**
     * 股东id,人存human-pid,公司存储company_id
     */
    private String shareholderId;

    /**
     * 股东实体类型：1-company, 2-human, 3-abnormal entity
     */
    private Integer shareholderEntityType;

    /**
     * 股东的gid
     */
    private Long shareholderNameId;

    /**
     * 投资总比例
     */
    private BigDecimal investmentRatioTotal;

    /**
     * 是否控制人：1-controller, 0-not controller
     */
    private Integer isController;

    /**
     * 是否最终控制人：1-ultimate, 0-not ultimate
     */
    private Integer isUltimate;

    /**
     * 是否大股东：1-is big_shareholder, 0-not big_shareholder
     */
    private Integer isBigShareholder;

    /**
     * 是否控股股东：1-is controlling_shareholder, 0-not controlling_shareholder
     */
    private Integer isControllingShareholder;

    /**
     * 股权持有路径：company->shareholder
     */
    private String equityHoldingPath;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 更新时间
     */
    private Date updateTime;

    /**
     * 是否已删除：1-deleted, 0-not deleted
     */
    private Integer isDeleted;


    @Override
    public int hashCode() {
        return Objects.hash(
                companyId, shareholderEntityType, shareholderId
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { // 自反性
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) { // 类型检查
            return false;
        }
        // 转换类型
        RatioPathCompanyPathString other = (RatioPathCompanyPathString) obj;
        // 比较各个属性是否相等
        return Objects.equals(companyId, other.companyId) &&
                Objects.equals(shareholderEntityType, other.shareholderEntityType) &&
                Objects.equals(shareholderId, other.shareholderId);
    }

    /**
     * 边的唯一键
     */
    public String getKey() {
        return String.format("%s#%s#%s", companyId, shareholderEntityType, shareholderId);
    }


}
