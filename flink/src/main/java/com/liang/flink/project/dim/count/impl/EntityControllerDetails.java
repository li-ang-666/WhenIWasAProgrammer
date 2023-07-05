package com.liang.flink.project.dim.count.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.dim.count.dao.EntityControllerDetailsDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EntityControllerDetails extends AbstractDataUpdate<HbaseOneRow> {
    private final EntityControllerDetailsDao dao = new EntityControllerDetailsDao();

    @Override
    public List<HbaseOneRow> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String shareholderId = String.valueOf(columnMap.get("tyc_unique_entity_id"));
        String count = dao.queryCount(shareholderId);
        HbaseSchema hbaseSchema = HbaseSchema.builder()
                .namespace("prism_c")
                .tableName("human_all_count")
                .columnFamily("cf")
                .rowKeyReverse(false)
                .build();
        HbaseOneRow row = new HbaseOneRow(hbaseSchema, shareholderId)
                .put("entity_controller_details_count", count);
        return Collections.singletonList(row);
    }

    @Override
    public List<HbaseOneRow> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
