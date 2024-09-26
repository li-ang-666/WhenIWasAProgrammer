package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import com.liang.common.util.JsonUtils;
import lombok.Data;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

@Data
public class RepairState {
    // forever positive
    private final Map<RepairTask, State> states = new ConcurrentSkipListMap<>(
            (task1, task2) -> task1.equals(task2) ? 0 : 1
    );

    public RepairState(List<RepairTask> repairTasks) {
        for (RepairTask repairTask : repairTasks) {
            this.states.put(repairTask, new State());
        }
    }

    public void initializeState(RepairState restored) {
        restored.states.forEach((k, v) -> {
            if (this.states.containsKey(k)) {
                this.states.put(k, v);
            }
        });
    }

    public void snapshotState(RepairTask repairTask, Roaring64Bitmap bitmap) {
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

    public String toReportString() {
        RepairState repairState = this;
        return JsonUtils.toString(
                states.keySet()
                        .stream()
                        .map(k -> new HashMap<String, Object>() {{
                            put("task", k);
                            put("position", repairState.getPosition(k));
                            put("count", repairState.getCount(k));
                        }})
                        .collect(Collectors.toList())
        );
    }

    @Data
    private static final class State {
        private volatile long position = 0L;
        private volatile long count = 0L;
    }
}
