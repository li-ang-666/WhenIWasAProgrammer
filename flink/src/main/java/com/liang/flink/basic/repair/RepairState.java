package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import com.liang.common.util.JsonUtils;
import lombok.Data;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Data
public class RepairState {
    private final Map<RepairTask, State> states = new ConcurrentSkipListMap<>();

    // 初始化
    public RepairState(List<RepairTask> repairTasks) {
        repairTasks.forEach(repairTask -> this.states.put(repairTask, new State()));
    }

    // 恢复
    public void restoreState(RepairState oldState) {
        oldState.states.forEach((k, v) -> {
            if (this.states.containsKey(k)) {
                this.states.put(k, v);
            }
        });
    }

    // 更新
    public void updateState(RepairTask repairTask, Roaring64Bitmap allIdBitmap, long position) {
        State state = states.get(repairTask);
        state.setAllIdBitmap(allIdBitmap);
        state.setPosition(position);
    }

    public Roaring64Bitmap getAllIdBitmap(RepairTask repairTask) {
        return states.get(repairTask).getAllIdBitmap();
    }

    public long getPosition(RepairTask repairTask) {
        return states.get(repairTask).getPosition();
    }

    public long getTotal(RepairTask repairTask) {
        return states.get(repairTask).getAllIdBitmap().getLongCardinality();
    }

    public String toReportString() {
        RepairState repairState = this;
        return JsonUtils.toString(
                states.keySet()
                        .stream()
                        .map(k -> new LinkedHashMap<String, Object>() {{
                            put("source", k.getSourceName());
                            put("table", k.getTableName());
                            long position = repairState.getPosition(k);
                            put("position", String.format("%,d", position));
                            AtomicLong count = new AtomicLong(0L);
                            repairState.getAllIdBitmap(k).forEach(id -> {
                                if (id <= position) {
                                    count.incrementAndGet();
                                }
                            });
                            put("count", count.get());
                            put("total", String.format("%,d", repairState.getTotal(k)));
                        }})
                        .collect(Collectors.toList())
        );
    }

    @Data
    private static final class State {
        private volatile Roaring64Bitmap allIdBitmap = new Roaring64Bitmap();
        private volatile long position = -1L;
    }
}
