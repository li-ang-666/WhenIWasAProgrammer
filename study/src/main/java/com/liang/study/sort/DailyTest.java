package com.liang.study.sort;

import org.junit.Test;

public class DailyTest {
    @Test
    public void test() {
        ArrayUtils.testSort(this::sort);
    }

    public void sort(int[] arr) {
    }

    private void quickSort(int[] arr, final int l, final int r) {
        if (l >= r) {
            return;
        }
        int p1 = l, p2 = r, pivot = arr[l];
        while (p1 < p2) {
            while (p1 < p2 && arr[p2] >= pivot) {
                p2--;
            }
            while (p1 < p2 && arr[p1] <= pivot) {
                p1++;
            }
            if (p1 < p2) {
                ArrayUtils.swap(arr, p1, p2);
            } else {
                ArrayUtils.swap(arr, p1, l);
            }
        }
        quickSort(arr, l, p1 - 1);
        quickSort(arr, p1 + 1, r);
    }
}
