package com.liang.study.sort;

import org.junit.Test;

public class DailyTest {
    @Test
    public void test() {
        ArrayUtils.testSort(this::sort);
    }

    public void sort(int[] arr) {
        int length = arr.length;
        for (int i = 0; i < length; i++) {
            boolean changed = false;
            for (int j = 0; j < length - i - 1; j++) {
                if (arr[j + 1] < arr[j]) {
                    ArrayUtils.swap(arr, j, j + 1);
                    changed = true;
                }
            }
            if (!changed) {
                break;
            }
        }
    }
}
