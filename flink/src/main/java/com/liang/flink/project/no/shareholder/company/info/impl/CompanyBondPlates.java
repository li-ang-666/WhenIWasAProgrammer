package com.liang.flink.project.no.shareholder.company.info.impl;

import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.no.shareholder.company.info.dao.NoShareholderDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CompanyBondPlates extends AbstractDataUpdate<Map<String, Object>> {
    private final NoShareholderDao dao = new NoShareholderDao();

    @Override
    public List<Map<String, Object>> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        String companyId = String.valueOf(singleCanalBinlog.getColumnMap().get("company_id"));
        String graphId = dao.queryCompanyId(companyId);

        if (StringUtils.isNumeric(graphId) && !"0".equals(graphId)) {
            dao.triggerCompanyIndex(graphId);
        }
        return new ArrayList<>();
    }

    @Override
    public List<Map<String, Object>> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}