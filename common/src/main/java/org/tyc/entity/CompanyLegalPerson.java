package org.tyc.entity;

import java.io.Serializable;
import java.util.Date;

/**
 * 企业法人表
 *
 * @TableName company_legal_person
 */
public class CompanyLegalPerson implements Serializable {
    private static final long serialVersionUID = 1L;
    /**
     * 自增id
     */
    private Long id;
    /**
     * 法人表所属公司id
     */
    private Long companyId;
    /**
     * 法人类型，1代表公司，2代表人，3非人非公司
     */
    private Integer legalRepType;
    /**
     * 法人是公司类型,公司id
     */
    private Long legalRepNameId;
    /**
     * 法人是人类型时的人pid
     */
    private String legalRepHumanId;
    /**
     * 公司类型法人名称
     */
    private String legalRepName;
    /**
     * 法定代表人字段展示名称
     */
    private String legalRepDisplayName;
    /**
     * 记录创建时间
     */
    private Date createTime;
    /**
     * 记录更新时间
     */
    private Date updateTime;

    /**
     * 自增id
     */
    public Long getId() {
        return id;
    }

    /**
     * 自增id
     */
    public void setId(Long id) {
        this.id = id;
    }

    /**
     * 法人表所属公司id
     */
    public Long getCompanyId() {
        return companyId;
    }

    /**
     * 法人表所属公司id
     */
    public void setCompanyId(Long companyId) {
        this.companyId = companyId;
    }

    /**
     * 法人类型，1代表公司，2代表人，3非人非公司
     */
    public Integer getLegalRepType() {
        return legalRepType;
    }

    /**
     * 法人类型，1代表公司，2代表人，3非人非公司
     */
    public void setLegalRepType(Integer legalRepType) {
        this.legalRepType = legalRepType;
    }

    /**
     * 法人是公司类型,公司id
     */
    public Long getLegalRepNameId() {
        return legalRepNameId;
    }

    /**
     * 法人是公司类型,公司id
     */
    public void setLegalRepNameId(Long legalRepNameId) {
        this.legalRepNameId = legalRepNameId;
    }

    /**
     * 法人是人类型时的人pid
     */
    public String getLegalRepHumanId() {
        return legalRepHumanId;
    }

    /**
     * 法人是人类型时的人pid
     */
    public void setLegalRepHumanId(String legalRepHumanId) {
        this.legalRepHumanId = legalRepHumanId;
    }

    /**
     * 公司类型法人名称
     */
    public String getLegalRepName() {
        return legalRepName;
    }

    /**
     * 公司类型法人名称
     */
    public void setLegalRepName(String legalRepName) {
        this.legalRepName = legalRepName;
    }

    /**
     * 法定代表人字段展示名称
     */
    public String getLegalRepDisplayName() {
        return legalRepDisplayName;
    }

    /**
     * 法定代表人字段展示名称
     */
    public void setLegalRepDisplayName(String legalRepDisplayName) {
        this.legalRepDisplayName = legalRepDisplayName;
    }

    /**
     * 记录创建时间
     */
    public Date getCreateTime() {
        return createTime;
    }

    /**
     * 记录创建时间
     */
    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    /**
     * 记录更新时间
     */
    public Date getUpdateTime() {
        return updateTime;
    }

    /**
     * 记录更新时间
     */
    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that == null) {
            return false;
        }
        if (getClass() != that.getClass()) {
            return false;
        }
        CompanyLegalPerson other = (CompanyLegalPerson) that;
        return (this.getId() == null ? other.getId() == null : this.getId().equals(other.getId()))
                && (this.getCompanyId() == null ? other.getCompanyId() == null : this.getCompanyId().equals(other.getCompanyId()))
                && (this.getLegalRepType() == null ? other.getLegalRepType() == null : this.getLegalRepType().equals(other.getLegalRepType()))
                && (this.getLegalRepNameId() == null ? other.getLegalRepNameId() == null : this.getLegalRepNameId().equals(other.getLegalRepNameId()))
                && (this.getLegalRepHumanId() == null ? other.getLegalRepHumanId() == null : this.getLegalRepHumanId().equals(other.getLegalRepHumanId()))
                && (this.getLegalRepName() == null ? other.getLegalRepName() == null : this.getLegalRepName().equals(other.getLegalRepName()))
                && (this.getLegalRepDisplayName() == null ? other.getLegalRepDisplayName() == null : this.getLegalRepDisplayName().equals(other.getLegalRepDisplayName()))
                && (this.getCreateTime() == null ? other.getCreateTime() == null : this.getCreateTime().equals(other.getCreateTime()))
                && (this.getUpdateTime() == null ? other.getUpdateTime() == null : this.getUpdateTime().equals(other.getUpdateTime()));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getId() == null) ? 0 : getId().hashCode());
        result = prime * result + ((getCompanyId() == null) ? 0 : getCompanyId().hashCode());
        result = prime * result + ((getLegalRepType() == null) ? 0 : getLegalRepType().hashCode());
        result = prime * result + ((getLegalRepNameId() == null) ? 0 : getLegalRepNameId().hashCode());
        result = prime * result + ((getLegalRepHumanId() == null) ? 0 : getLegalRepHumanId().hashCode());
        result = prime * result + ((getLegalRepName() == null) ? 0 : getLegalRepName().hashCode());
        result = prime * result + ((getLegalRepDisplayName() == null) ? 0 : getLegalRepDisplayName().hashCode());
        result = prime * result + ((getCreateTime() == null) ? 0 : getCreateTime().hashCode());
        result = prime * result + ((getUpdateTime() == null) ? 0 : getUpdateTime().hashCode());
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" [");
        sb.append("Hash = ").append(hashCode());
        sb.append(", id=").append(id);
        sb.append(", companyId=").append(companyId);
        sb.append(", legalRepType=").append(legalRepType);
        sb.append(", legalRepNameId=").append(legalRepNameId);
        sb.append(", legalRepHumanId=").append(legalRepHumanId);
        sb.append(", legalRepName=").append(legalRepName);
        sb.append(", legalRepDisplayName=").append(legalRepDisplayName);
        sb.append(", createTime=").append(createTime);
        sb.append(", updateTime=").append(updateTime);
        sb.append(", serialVersionUID=").append(serialVersionUID);
        sb.append("]");
        return sb.toString();
    }
}