package com.liang.flink.project.bdp.equity.impl;

import com.liang.common.service.SQL;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.bdp.equity.dao.BdpEquityDao;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TycEntityMainReference extends AbstractDataUpdate<SQL> {
    private final BdpEquityDao dao = new BdpEquityDao();

    @Override
    public List<SQL> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String entityId = String.valueOf(columnMap.get("tyc_unique_entity_id"));
        String entityName = String.valueOf(columnMap.get("entity_name_valid"));
        //股东
        if (StringUtils.isNotBlank(entityName) && !"null".equals(entityName)) {
            dao.updateShareholderName(entityId, entityName)
            //公司
            if (StringUtils.isNumeric(entityId)) {
                dao.updateCompanyName(entityId, entityName);
            }
        }
        return new ArrayList<>();
    }

    @Override
    public List<SQL> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return new ArrayList<>();
    }
}
