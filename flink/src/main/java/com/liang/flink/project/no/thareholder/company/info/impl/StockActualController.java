package com.liang.flink.project.no.thareholder.company.info.impl;

import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.no.thareholder.company.info.dao.NoShareholderDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StockActualController extends AbstractDataUpdate<Map<String, Object>> {
    private final NoShareholderDao dao = new NoShareholderDao();

    @Override
    public List<Map<String, Object>> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        String companyId = String.valueOf(singleCanalBinlog.getColumnMap().get("graph_id"));
        if (StringUtils.isNumeric(companyId) && !"0".equals(companyId)) {
            dao.triggerCompanyIndex(companyId);
        }
        return new ArrayList<>();
    }
}
