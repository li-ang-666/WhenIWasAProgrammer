package com.liang.study.sort;

import org.junit.Test;

public class HeapSort {
    @Test
    public void test() {
        ArrayUtils.test(this::sort);
    }

    public void sort(int[] arr) {
        //建堆
        int n = arr.length;
        for (int i = (n - 2) / 2; i >= 0; i--) {
            heapify(arr, n, i);
        }
        //排序
        for (int i = arr.length - 1; i >= 1; i--) {
            //起始是成型堆, 交换顶和尾, 对堆顶heapify, 数组去一个尾, 进入下次循环
            ArrayUtils.swap(arr, 0, i);
            heapify(arr, i, 0);
        }
    }

    public void heapify(int[] arr, int n, int i) {
        int max = i;
        int l = 2 * i + 1;
        int r = 2 * i + 2;
        if (l <= n - 1 && arr[l] > arr[max])
            max = l;
        if (r <= n - 1 && arr[r] > arr[max])
            max = r;
        //如果发生变化, i变为正确, max变为未知, 继续往下heapify
        if (max != i) {
            ArrayUtils.swap(arr, max, i);
            heapify(arr, n, max);
        }
    }
}
