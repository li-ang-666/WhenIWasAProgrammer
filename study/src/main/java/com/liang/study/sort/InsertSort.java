package com.liang.study.sort;

import org.junit.Test;

public class InsertSort {
    @Test
    public void test() {
        ArrayUtils.testSort(this::sort);
    }

    public void sort(int[] arr) {
        for (int i = 1; i <= arr.length - 1; i++) {
            for (int j = i; j - 1 >= 0 && arr[j - 1] > arr[j]; j--) {
                ArrayUtils.swap(arr, j, j - 1);
            }
        }
    }
}
