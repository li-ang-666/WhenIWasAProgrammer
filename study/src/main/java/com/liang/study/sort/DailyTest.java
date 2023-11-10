package com.liang.study.sort;

import org.junit.Test;

public class DailyTest {
    @Test
    public void test() {
        ArrayUtils.testSort(this::sort);
    }

    public void sort(int[] arr) {
        int len = arr.length;
        int k = 3;
        for (int gap = len / k; gap >= 1; gap /= k) {
            for (int i = 0; i < len; i++) {
                for (int j = i; j - gap >= 0 && arr[j - gap] > arr[j]; j -= gap) {
                    ArrayUtils.swap(arr, j - gap, j);
                }
            }
        }
    }
}
