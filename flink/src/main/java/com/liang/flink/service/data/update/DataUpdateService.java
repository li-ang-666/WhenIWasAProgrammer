package com.liang.flink.service.data.update;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.flink.dto.SingleCanalBinlog;

import java.util.ArrayList;
import java.util.List;

public class DataUpdateService<OUT> {
    private final DataUpdateContext dataUpdateContext;

    public DataUpdateService(DataUpdateContext dataUpdateContext) {
        this.dataUpdateContext = dataUpdateContext;
    }

    @SuppressWarnings("unchecked")
    public List<OUT> invoke(SingleCanalBinlog singleCanalBinlog) {
        AbstractDataUpdate<OUT> impl = (AbstractDataUpdate<OUT>) dataUpdateContext.getClass(singleCanalBinlog.getTable());
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
