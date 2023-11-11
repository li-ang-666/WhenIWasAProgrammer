package com.liang.study.sort;

import org.junit.Test;

public class DailySortTest {
    @Test
    public void test() {
        ArrayUtils.testSort(this::sort);
    }

    public void sort(int[] arr) {
        int len = arr.length;
        for (int i = (len - 2) / 2; i >= 0; i--) {
            heapify(arr, i, len);
        }
        for (int i = len - 1; i >= 0; i--) {
            ArrayUtils.swap(arr, 0, i);
            heapify(arr, 0, i);
        }
    }

    private void heapify(int[] arr, int i, int len) {
        int max = i;
        int l = 2 * i + 1, r = 2 * i + 2;
        if (l < len && arr[max] < arr[l]) {
            max = l;
        }
        if (r < len && arr[max] < arr[r]) {
            max = r;
        }
        if (max != i) {
            ArrayUtils.swap(arr, max, i);
            heapify(arr, max, len);
        }
    }
}
