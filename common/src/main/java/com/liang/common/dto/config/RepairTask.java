package com.liang.common.dto.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class RepairTask implements Serializable {
    private String sourceName;
    private String tableName;
    private String columns = "*";
    private String where = "1 = 1";
    @NonNull
    private RepairTaskMode mode = RepairTaskMode.D;

    public enum RepairTaskMode implements Serializable {
        D, F
    }
}
