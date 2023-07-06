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
        if (shareholderId.matches("\\d+")) {
            HbaseSchema hbaseSchema = HbaseSchema.builder()
                    .namespace("prism_c")
                    .tableName("company_all_count")
                    .columnFamily("count")
                    .rowKeyReverse(true)
                    .build();
            HbaseOneRow row = new HbaseOneRow(hbaseSchema, shareholderId)
                    .put("entity_beneficiary_details_count", count);
            return Collections.singletonList(row);
        } else {
            HbaseSchema hbaseSchema = HbaseSchema.builder()
                    .namespace("prism_c")
                    .tableName("human_all_count")
                    .columnFamily("cf")
                    .rowKeyReverse(false)
                    .build();
            HbaseOneRow row = new HbaseOneRow(hbaseSchema, shareholderId)
                    .put("entity_beneficiary_details_count", count);
            return Collections.singletonList(row);
        }
    }

    @Override
    public List<HbaseOneRow> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
