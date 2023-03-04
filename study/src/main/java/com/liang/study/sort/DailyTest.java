package com.liang.study.sort;

import org.junit.Test;

public class DailyTest {
    @Test
    public void test() {
        ArrayUtils.test(this::sort);
    }

    public void sort(int[] arr) {
        int n = arr.length;
        for (int i = (n - 2) / 2; i >= 0; i--) {
            heapify(arr, n, i);
        }
        for (int i = n - 1; i >= 1; i--) {
            ArrayUtils.swap(arr, 0, i);
            heapify(arr, i, 0);
        }
    }

    public void heapify(int[] arr, int n, int i) {
        int max = i;
        int l = 2 * i + 1;
        int r = 2 * i + 2;
        if (l <= n - 1 && arr[l] > arr[max]) {
            max = l;
        }
        if (r <= n - 1 && arr[r] > arr[max]) {
            max = r;
        }
        if (max != i) {
            ArrayUtils.swap(arr, i, max);
            heapify(arr, n, max);
        }
    }
}
