package com.liang.flink.basic.repair;

import com.liang.common.dto.config.RepairTask;
import com.liang.common.util.JsonUtils;
import lombok.Data;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Data
public class RepairState implements Serializable {
    private final List<RepairTask> repairTasks;
    private final Map<RepairTask, State> states = new ConcurrentHashMap<>();

    // 初始化
    public RepairState(List<RepairTask> repairTasks) {
        this.repairTasks = repairTasks;
        repairTasks.forEach(repairTask -> states.put(repairTask, new State()));
    }

    // 恢复
    public void restoreState(RepairState oldState) {
        oldState.getStates().forEach((k, v) -> {
            if (states.containsKey(k)) {
                states.put(k, v);
            }
        });
    }

    // 更新
    public void updatePosition(RepairTask repairTask, long position) {
        State state = states.get(repairTask);
        state.setPosition(position);
    }

    public Roaring64Bitmap getAllIdBitmap(RepairTask repairTask) {
        return states.get(repairTask).getAllIdBitmap();
    }

    public long getPosition(RepairTask repairTask) {
        return states.get(repairTask).getPosition();
    }

    public long getCount(RepairTask repairTask) {
        return states.get(repairTask).getCount();
    }

    public long getTotal(RepairTask repairTask) {
        return states.get(repairTask).getTotal();
    }

    public String toReportString() {
        List<Map<String, Object>> infos = new ArrayList<>();
        repairTasks.forEach(repairTask -> {
            LinkedHashMap<String, Object> info = new LinkedHashMap<>();
            info.put("source", repairTask.getSourceName());
            info.put("table", repairTask.getTableName());
            info.put("position", String.format("%,d", getPosition(repairTask)));
            info.put("count", String.format("%,d", getCount(repairTask)));
            info.put("total", String.format("%,d", getTotal(repairTask)));
            infos.add(info);
        });
        return JsonUtils.toString(infos);
    }

    @Data
    private static final class State implements Serializable {
        private volatile Roaring64Bitmap allIdBitmap = new Roaring64Bitmap();
        private volatile long position = -1L;

        private long getCount() {
            AtomicLong count = new AtomicLong(0L);
            allIdBitmap.forEach(id -> {
                if (id <= position) {
                    count.incrementAndGet();
                }
            });
            return count.get();
        }

        private long getTotal() {
            return allIdBitmap.getLongCardinality();
        }
    }
}
