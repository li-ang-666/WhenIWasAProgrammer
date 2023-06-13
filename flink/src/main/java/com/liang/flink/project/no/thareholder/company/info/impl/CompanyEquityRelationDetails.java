package com.liang.flink.project.no.thareholder.company.info.impl;

import com.liang.common.util.DateTimeUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.no.thareholder.company.info.dao.NoShareholderDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CompanyEquityRelationDetails extends AbstractDataUpdate<Map<String, Object>> {
    private final NoShareholderDao dao = new NoShareholderDao();

    @Override
    public List<Map<String, Object>> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String companyId = String.valueOf(columnMap.get("company_id"));
        String year = String.valueOf(columnMap.get("reference_pt_year"));
        if (year.equals(DateTimeUtils.currentDate().substring(0, 4))) {
            dao.deleteCompany(companyId);
        }
        return new ArrayList<>();
    }
}
