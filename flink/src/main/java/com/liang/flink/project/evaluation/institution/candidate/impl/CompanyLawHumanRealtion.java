package com.liang.flink.project.evaluation.institution.candidate.impl;

import com.liang.common.service.SQL;
import com.liang.common.util.SqlUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.data.update.AbstractDataUpdate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CompanyLawHumanRealtion extends AbstractDataUpdate<String> {

    @Override
    public List<String> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String sourceTable = String.valueOf(columnMap.get("source_table"));
        List<String> sqls = new ArrayList<>();
        if ("zhixinginfo_evaluate".equals(sourceTable)) {
            String sql = new SQL().UPDATE("zhixinginfo_evaluate_index")
                    .SET("update_time = date_add(update_time, interval 1 second)")
                    .WHERE("main_id = " + SqlUtils.formatValue(columnMap.get("source_id")))
                    .toString();
            sqls.add(sql);
        }
        return sqls;
    }

    @Override
    public List<String> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
