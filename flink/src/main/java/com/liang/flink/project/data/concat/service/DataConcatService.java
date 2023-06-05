package com.liang.flink.project.data.concat.service;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.HbaseOneRow;
import com.liang.common.util.TableNameUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.AbstractDataUpdate;
import com.liang.flink.service.IDataUpdate;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DataConcatService implements Serializable {
    private final Map<String, AbstractDataUpdate> DataUpdateImpls = new HashMap<>();

    public DataConcatService() throws Exception {
        List<String> shortClassNames = new ArrayList<>();
        shortClassNames.add("RestrictConsumptionSplitIndex");
        shortClassNames.add("JudicialAssistanceIndex");
        shortClassNames.add("RestrictedOutboundIndex");
        shortClassNames.add("EquityPledgeReinvest");
        shortClassNames.add("EquityPledgeDetail");
        shortClassNames.add("CompanyBranch");
        for (String shortClassName : shortClassNames) {
            String fullClassName = "com.liang.flink.project.data.concat.impl."
                    + shortClassName;
            String tableName = TableNameUtils.humpToUnderLine(shortClassName);
            DataUpdateImpls.put(tableName, (AbstractDataUpdate) Class.forName(fullClassName).newInstance());
            log.info("表处理类加载: {} -> {}", tableName, shortClassName);
        }
    }

    public List<HbaseOneRow> invoke(SingleCanalBinlog singleCanalBinlog) throws Exception {
        String tableName = singleCanalBinlog.getTable();
        CanalEntry.EventType eventType = singleCanalBinlog.getEventType();

        IDataUpdate iDataUpdate = DataUpdateImpls.get(tableName);
        if (iDataUpdate == null) {
            log.warn("该表无处理类: {}", singleCanalBinlog.getTable());
            return new ArrayList<>();
        }
        if (eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE) {
            return iDataUpdate.updateWithReturn(singleCanalBinlog);
        } else {
            return iDataUpdate.deleteWithReturn(singleCanalBinlog);
        }
    }
}
