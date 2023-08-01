package com.liang.flink.project.annual.report.impl;

import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.annual.report.dao.AnnualReportDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ReportWebinfo extends AbstractDataUpdate<String> {
    private final AnnualReportDao dao = new AnnualReportDao();

    @Override
    public List<String> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        return null;
    }

    @Override
    public List<String> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        return Collections.singletonList(dao.delete("report_webinfo", columnMap.get("id")));
    }
}
