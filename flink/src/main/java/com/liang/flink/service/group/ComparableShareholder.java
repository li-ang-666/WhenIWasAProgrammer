package com.liang.flink.service.group;

import lombok.Data;
import lombok.NonNull;

import java.util.PriorityQueue;

@Data
public class ComparableShareholder implements Comparable<ComparableShareholder> {
    private final String companyId;
    private final Long registerCapitalAmt;
    private final Long groupSize;
    private final String establishDate;

    public static void main(String[] args) {
        PriorityQueue<ComparableShareholder> comparableShareholders = new PriorityQueue<>();
        comparableShareholders.add(new ComparableShareholder("1", 1L, 1L, "2021"));
        comparableShareholders.add(new ComparableShareholder("2", 1L, 1L, "2022"));
        comparableShareholders.add(new ComparableShareholder("3", 1L, 1L, "2023"));
        comparableShareholders.add(new ComparableShareholder("4", 1L, 1L, "2024"));
        while (!comparableShareholders.isEmpty()) {
            System.out.println(comparableShareholders.poll());
        }
    }

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


