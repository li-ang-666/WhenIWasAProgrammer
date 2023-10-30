package com.liang.flink.project.annual.report.impl;

import cn.hutool.core.util.ObjUtil;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.annual.report.dao.AnnualReportDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TycEntityGeneralPropertyReference extends AbstractDataUpdate<String> {
    private final AnnualReportDao dao = new AnnualReportDao();

    @Override
    public List<String> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
        Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
        boolean nameEquals = ObjUtil.equals(beforeColumnMap.get("entity_name_valid"), afterColumnMap.get("entity_name_valid"));
        if (!nameEquals) {
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String id = String.valueOf(columnMap.get("id"));
            dao.updateSource(id);
        }
        return new ArrayList<>();
    }

    @Override
    public List<String> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
