package com.liang.flink.service.group;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

@Data
@Builder
public class ComparableShareholder implements Comparable<ComparableShareholder> {
    private String id;
    private String name;
    private Long registerCapitalAmt;
    private Long controllingSize;
    private String establishDate;

    @Override
    public int compareTo(@NonNull ComparableShareholder another) {
        if (this.registerCapitalAmt.compareTo(another.registerCapitalAmt) == 0) {
            if (this.controllingSize.compareTo(another.getControllingSize()) == 0) {
                return this.establishDate.compareTo(another.establishDate);
            } else {
                return -this.controllingSize.compareTo(another.getControllingSize());
            }
        } else {
            return -this.registerCapitalAmt.compareTo(another.registerCapitalAmt);
        }
    }
}
