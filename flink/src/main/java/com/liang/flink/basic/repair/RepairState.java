package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import lombok.Data;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class RepairState {
    public RepairState(List<RepairTask> repairTasks) {
        for (RepairTask repairTask : repairTasks) {
            states.put(repairTask, new State());
        }
    }

    private Map<RepairTask, State> states = new HashMap<>();

    public void register(RepairTask repairTask, Roaring64Bitmap bitmap) {
        State state = states.get(repairTask);
        state.setPosition(bitmap.last());
        state.setCount(state.getCount() + bitmap.getLongCardinality());
    }

    public long getPosition(RepairTask repairTask) {
        return states.get(repairTask).getPosition();
    }

    public long getCount(RepairTask repairTask) {
        return states.get(repairTask).getCount();
    }

    @Data
    private static final class State {
        private long position = 0L;
        private long count = 0L;
    }
}
