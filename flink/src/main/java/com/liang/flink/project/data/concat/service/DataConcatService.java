package com.liang.flink.project.data.concat.service;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.HbaseOneRow;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.AbstractDataUpdate;
import com.liang.flink.service.Context;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class DataConcatService implements Serializable {
    private final Context context;

    public DataConcatService() throws Exception {
        context = new Context("com.liang.flink.project.data.concat.impl")
                .addClass("RestrictConsumptionSplitIndex")
                .addClass("JudicialAssistanceIndex")
                .addClass("RestrictedOutboundIndex")
                .addClass("EquityPledgeReinvest")
                .addClass("EquityPledgeDetail")
                .addClass("CompanyBranch");
    }

    public List<HbaseOneRow> invoke(SingleCanalBinlog singleCanalBinlog) {
        AbstractDataUpdate impl = context.getClass(singleCanalBinlog.getTable());
        if (impl == null) {
            return new ArrayList<>();
        }
        CanalEntry.EventType eventType = singleCanalBinlog.getEventType();
        if (eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE) {
            return impl.updateWithReturn(singleCanalBinlog);
        } else {
            return impl.deleteWithReturn(singleCanalBinlog);
        }
    }
}
