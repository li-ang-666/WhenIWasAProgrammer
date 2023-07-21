package org.tyc;

import com.alibaba.fastjson.JSONArray;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.tyc.entity.RatioPathCompany;
import org.tyc.mybatis.mapper.company_legal_person.CompanyLegalPersonMapper;
import org.tyc.mybatis.mapper.share_holder_label.InvestmentRelationMapper;
import org.tyc.mybatis.mapper.share_holder_label.PersonnelEmploymentHistoryMapper;
import org.tyc.mybatis.mapper.share_holder_label.RatioPathCompanyMapper;
import org.tyc.mybatis.runner.JDBCRunner;
import org.tyc.utils.InvestUtil;
import org.tyc.utils.PathFormatter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

@Slf4j
public class RatioPathCompanyTrigger {
    private final InvestmentRelationMapper investmentRelationMapper = JDBCRunner.getMapper(InvestmentRelationMapper.class, "e1d4c0a1d8d1456ba4b461ab8b9f293din01/prism_shareholder_path.xml");
    private final PersonnelEmploymentHistoryMapper personnelEmploymentHistoryMapper = JDBCRunner.getMapper(PersonnelEmploymentHistoryMapper.class, "36c607bfd9174d4e81512aa73375f0fain01/human_base.xml");
    private final CompanyLegalPersonMapper companyLegalPersonMapper = JDBCRunner.getMapper(CompanyLegalPersonMapper.class, "ee59dd05fc0f4bb9a2497c8d9146a53cin01/company_base.xml");
    private final RatioPathCompanyMapper ratioPathCompanyMapper = JDBCRunner.getMapper(RatioPathCompanyMapper.class, "e1d4c0a1d8d1456ba4b461ab8b9f293din01/prism_shareholder_path.xml");

    public static void main(String[] args) {
        RatioPathCompanyTrigger trigger = new RatioPathCompanyTrigger();
        trigger.trigger(Arrays.asList(2318455639L));
    }

    public void trigger(List<Long> companyIds) {
        HashSet<Long> set = new HashSet<>(companyIds);
        companyIds.stream()
                .distinct()
                .map(companyId -> {
                    try {
                        return QueryAllShareHolderFromCompanyIdObj.queryAllShareHolderFromCompanyId(companyId, 10000, investmentRelationMapper, personnelEmploymentHistoryMapper, companyLegalPersonMapper);
                    } catch (Exception e) {
                        log.error("queryAllShareHolderFromCompanyId({}) error", companyId);
                        return null;
                    }
                })
                .filter(map -> map != null && !map.isEmpty())
                .flatMap(map -> map.values().stream())
                .map(investmentRelation -> {
                    RatioPathCompany ratioPathCompany = InvestUtil.convertInvestmentRelationToRatioPathCompany(investmentRelation);
                    JSONArray jsonArray;
                    try {
                        jsonArray = PathFormatter.formatInvestmentRelationToPath(investmentRelation);
                    } catch (Exception e) {
                        log.error("formatInvestmentRelationToPath({}) error", investmentRelation);
                        jsonArray = new JSONArray();
                    }
                    return Tuple2.of(ratioPathCompany, jsonArray);
                })
                .filter(tuple2 -> {
                    RatioPathCompany ratioPathCompany = tuple2.f0;
                    Long companyId = ratioPathCompany.getCompanyId();
                    Integer shareholderEntityType = ratioPathCompany.getShareholderEntityType();
                    String shareholderId = ratioPathCompany.getShareholderId();
                    return companyId != null
                            && (shareholderEntityType == 1 || shareholderEntityType == 2)
                            && StringUtils.isNotBlank(shareholderId);
                })
                .forEach(tuple2 -> {
                    RatioPathCompany ratioPathCompany = tuple2.f0;
                    JSONArray path = tuple2.f1;
                    Long companyId = ratioPathCompany.getCompanyId();
                    if (set.contains(companyId)) {
                        set.remove(companyId);
                        ratioPathCompanyMapper.deleteByCompanyIdLimit200(companyId);
                    }
                    ratioPathCompanyMapper.insertRatioPathCompanyWithNewPathString(
                            companyId,
                            ratioPathCompany.getShareholderId(),
                            ratioPathCompany.getShareholderEntityType(),
                            ratioPathCompany.getShareholderNameId(),
                            ratioPathCompany.getInvestmentRatioTotal(),
                            ratioPathCompany.getIsController(),
                            ratioPathCompany.getIsUltimate(),
                            ratioPathCompany.getIsBigShareholder(),
                            ratioPathCompany.getIsControllingShareholder(),
                            path.toString(),
                            ratioPathCompany.getIsDeleted()
                    );
                });
        set.forEach(ratioPathCompanyMapper::deleteByCompanyIdLimit200);
    }
}
