package com.liang.flink.project.dim.count.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.util.TycUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.dim.count.dao.DimCountShareholderDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RatioPathCompany extends AbstractDataUpdate<HbaseOneRow> {
    private final DimCountShareholderDao dao = new DimCountShareholderDao();

    @Override
    public List<HbaseOneRow> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        List<HbaseOneRow> result = new ArrayList<>();
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String companyId = String.valueOf(columnMap.get("company_id"));
        if (TycUtils.isUnsignedId(companyId)) {
            HbaseOneRow hbaseOneRow = new HbaseOneRow(HbaseSchema.COMPANY_ALL_COUNT, companyId)
                    .put("has_controller", dao.queryHasController(companyId))
                    .put("has_beneficiary", dao.queryHasBeneficiary(companyId))
                    .put("num_control_ability", dao.queryNumControlAbility(companyId));
            result.add(hbaseOneRow);
        }
        String shareholderId = String.valueOf(columnMap.get("shareholder_id"));
        if (TycUtils.isTycUniqueEntityId(shareholderId)) {
            if (StringUtils.isNumeric(shareholderId)) {
                HbaseOneRow hbaseOneRow = new HbaseOneRow(HbaseSchema.COMPANY_ALL_COUNT, shareholderId)
                        .put("has_controller", dao.queryHasController(shareholderId))
                        .put("has_beneficiary", dao.queryHasBeneficiary(shareholderId))
                        .put("num_control_ability", dao.queryNumControlAbility(shareholderId));
                result.add(hbaseOneRow);
            } else {
                HbaseOneRow hbaseOneRow = new HbaseOneRow(HbaseSchema.HUMAN_ALL_COUNT, shareholderId)
                        .put("num_control_ability", dao.queryNumControlAbility(shareholderId))
                        .put("num_benefit_ability", dao.queryNumBenefitAbility(shareholderId));
                result.add(hbaseOneRow);
            }
        }
        return result;
    }

    @Override
    public List<HbaseOneRow> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
