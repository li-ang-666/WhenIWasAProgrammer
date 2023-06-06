package com.liang.flink.service.data.update;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.util.JsonUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class DataUpdateService<OUT> {
    private final DataUpdateContext<OUT> dataUpdateContext;

    public DataUpdateService(DataUpdateContext<OUT> dataUpdateContext) {
        this.dataUpdateContext = dataUpdateContext;
    }

    public List<OUT> invoke(SingleCanalBinlog singleCanalBinlog) {
        //log.debug("dataUpdateService input: {}", JsonUtils.toString(singleCanalBinlog));
        AbstractDataUpdate<OUT> impl = dataUpdateContext.getClass(singleCanalBinlog.getTable());
        List<OUT> out = new ArrayList<>();
        CanalEntry.EventType eventType = singleCanalBinlog.getEventType();
        if (impl != null && (eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE)) {
            out.addAll(impl.updateWithReturn(singleCanalBinlog));
        } else if (impl != null && eventType == CanalEntry.EventType.DELETE) {
            out.addAll(impl.deleteWithReturn(singleCanalBinlog));
        }
        //log.debug("dataUpdateService output: {}", JsonUtils.toString(out));
        return out;
    }
}
