package com.liang.flink.project.dim.count.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.dim.count.dao.EntityBeneficiaryDetailsDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EntityBeneficiaryDetails extends AbstractDataUpdate<HbaseOneRow> {
    private final EntityBeneficiaryDetailsDao dao = new EntityBeneficiaryDetailsDao();

    @Override
    public List<HbaseOneRow> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String shareholderId = String.valueOf(columnMap.get("tyc_unique_entity_id_beneficiary"));
        String count = dao.queryCount(shareholderId);
        HbaseOneRow row;
        if (shareholderId.matches("\\d+")) {
            row = new HbaseOneRow(HbaseSchema.COMPANY_ALL_COUNT, shareholderId)
                    .put("entity_beneficiary_details_count", count);
        } else {
            row = new HbaseOneRow(HbaseSchema.HUMAN_ALL_COUNT, shareholderId)
                    .put("entity_beneficiary_details_count", count);
        }
        return Collections.singletonList(row);
    }

    @Override
    public List<HbaseOneRow> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
