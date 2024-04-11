package com.liang.flink.service.group;

import lombok.Data;
import lombok.NonNull;

@Data
public class ComparableShareholder implements Comparable<ComparableShareholder> {
    private final String companyId;
    private final Long registerCapitalAmt;
    private final Long groupSize;
    private final String establishDate;

    @Override
    public int compareTo(@NonNull ComparableShareholder another) {
        if (this.registerCapitalAmt.compareTo(another.registerCapitalAmt) == 0) {
            if (this.groupSize.compareTo(another.getGroupSize()) == 0) {
                return this.establishDate.compareTo(another.establishDate);
            } else {
                return -this.groupSize.compareTo(another.getGroupSize());
            }
        } else {
            return -this.registerCapitalAmt.compareTo(another.registerCapitalAmt);
        }
    }
}


