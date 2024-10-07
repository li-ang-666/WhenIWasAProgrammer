package com.liang.common.dto.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class RepairTask implements Serializable, Comparable<RepairTask> {
    private String sourceName;
    private String tableName;
    private String columns = "*";
    private String where = "1 = 1";
    private RepairTaskMode mode = RepairTaskMode.D;

    @Override
    public int compareTo(@NonNull RepairTask another) {
        // forever positive
        return this.equals(another) ? 0 : 1;
    }

    public enum RepairTaskMode {
        D, F
    }
}
