package org.tyc.mybatis.mapper.share_holder_label;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.tyc.entity.RatioPathCompanyPathString;

import java.math.BigDecimal;
import java.util.List;

public interface RatioPathCompanyMapper {

    //按照company_id删除 RatioPathCompany 表中的记录
    @Delete("DELETE FROM ratio_path_company WHERE company_id = #{companyId} limit 200")
    int deleteByCompanyIdLimit200(Long companyId);

    @Insert("INSERT INTO ratio_path_company" +
            "(company_id, shareholder_id, shareholder_entity_type, shareholder_name_id," +
            "investment_ratio_total, is_controller, is_ultimate, is_big_shareholder," +
            "is_controlling_shareholder, equity_holding_path, is_deleted)" +
            "VALUES (#{companyId}, #{shareholderId}, #{shareholderEntityType}, #{shareholderNameId}," +
            "#{investmentRatioTotal}, #{isController}, #{isUltimate}, #{isBigShareholder}," +
            "#{isControllingShareholder}, #{equityHoldingPath},  #{isDeleted})"
    )
    void insertRatioPathCompanyWithNewPathString(
            @Param("companyId") Long companyId,
            @Param("shareholderId") String shareholderId,
            @Param("shareholderEntityType") Integer shareholderEntityType,
            @Param("shareholderNameId") Long shareholderNameId,
            @Param("investmentRatioTotal") BigDecimal investmentRatioTotal,
            @Param("isController") Integer isController,
            @Param("isUltimate") Integer isUltimate,
            @Param("isBigShareholder") Integer isBigShareholder,
            @Param("isControllingShareholder") Integer isControllingShareholder,
            @Param("equityHoldingPath") String equityHoldingPath,
            @Param("isDeleted") Integer isDeleted);

    @Select("select * from ratio_path_company  WHERE company_id = #{companyId} and is_deleted = 0 and is_ultimate = 1")
    List<RatioPathCompanyPathString> getUltimateRatioPathCompanyByCompanyId(@Param("companyId") String companyId);

    @Select("select * from ratio_path_company  WHERE company_id = #{companyId} and is_deleted = 0 and is_controller = 1")
    List<RatioPathCompanyPathString> getControllerRatioPathCompanyByCompanyId(@Param("companyId") String companyId);

    @Select("select company_id from ratio_path_company  WHERE shareholder_id = #{shareHolderId} and is_deleted = 0 and is_controller = 1")
    List<String> getCompanyIdByShareHolderId(@Param("shareHolderId") String shareHolderId);

    @Select("select company_id from ratio_path_company  WHERE shareholder_id = #{shareHolderId} and is_deleted = 0 and is_ultimate = 1")
    List<String> getUltimateCompanyIdByShareHolderId(@Param("shareHolderId") String shareHolderId);

}
