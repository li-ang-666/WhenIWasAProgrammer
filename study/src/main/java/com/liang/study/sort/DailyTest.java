package com.liang.study.sort;

import org.junit.Test;

public class DailyTest {
    @Test
    public void test() {
        ArrayUtils.testSort(this::sort);
    }

    public void sort(int[] arr) {
        int len = arr.length;
        for (int i = 0; i < len; i++) {
            for (int j = i; j - 1 >= 0 && arr[j - 1] > arr[j]; j--) {
                ArrayUtils.swap(arr, j - 1, j);
            }
        }
    }
}
