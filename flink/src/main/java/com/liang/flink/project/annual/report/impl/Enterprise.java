package com.liang.flink.project.annual.report.impl;

import cn.hutool.core.util.ObjUtil;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.annual.report.dao.AnnualReportDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Enterprise extends AbstractDataUpdate<String> {
    private final AnnualReportDao dao = new AnnualReportDao();

    @Override
    public List<String> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
        Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
        boolean deletedEquals = ObjUtil.equals(beforeColumnMap.get("deleted"), afterColumnMap.get("deleted"));
        boolean nameEquals = ObjUtil.equals(beforeColumnMap.get("name"), afterColumnMap.get("name"));
        if (!deletedEquals || !nameEquals) {
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
