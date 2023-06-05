package com.liang.flink.project.data.concat.impl;


import com.liang.common.dto.HbaseOneRow;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.data.concat.dao.CompanyBranchDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CompanyBranch extends AbstractDataUpdate<HbaseOneRow> {
    private final CompanyBranchDao dao = new CompanyBranchDao();

    @Override
    public List<HbaseOneRow> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        List<HbaseOneRow> result = new ArrayList<>();
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String companyId = String.valueOf(columnMap.get("company_id"));
        if (StringUtils.isNumeric(companyId) && !"0".equals(companyId)) {
            HbaseOneRow hbaseOneRow = new HbaseOneRow("dataConcat", companyId);
            hbaseOneRow
                    .put("company_branch_total_branch", dao.queryTotalBranch(companyId))
                    .put("company_branch_total_canceled_branch", dao.queryTotalCanceledBranch(companyId))
                    .put("company_branch_most_year", dao.queryMostYear(companyId))
                    .put("company_branch_most_area", dao.queryMostArea(companyId));
            result.add(hbaseOneRow);
        }
        return result;
    }

    @Override
    public List<HbaseOneRow> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
