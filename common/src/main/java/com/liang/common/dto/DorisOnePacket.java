package com.liang.common.dto;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class DorisOnePacket {
    private final Map<String, String> putHeaders = new HashMap<>();
    private final List<String> derivedCols = new ArrayList<>();
    private final List<Map<String, Object>> contents;

    public DorisOnePacket(List<Map<String, Object>> contents) {
        this.contents = contents;
    }

    public DorisOnePacket uniqueDeleteOn(String condition) {
        putHeaders.put("delete", condition);
        return this;
    }

    public DorisOnePacket uniqueOrderBy(String sequenceCol) {
        putHeaders.put("function_column.sequence_col", sequenceCol);
        return this;
    }

    public DorisOnePacket addDerivedCol(String derivedCol) {
        derivedCols.add(derivedCol);
        return this;
    }
}
