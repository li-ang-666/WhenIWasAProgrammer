package com.liang.study.base.sort;

import cn.hutool.core.util.ArrayUtil;

import java.util.function.Consumer;

public class HeapSort implements Consumer<int[]> {
    public static void main(String[] args) {
        SortUtils.testSort(new HeapSort());
    }

    @Override
    public void accept(int[] arr) {
        int len = arr.length;
        // 建堆
        for (int i = (len - 2) / 2; i >= 0; i--) {
            heapify(arr, i, len);
        }
        // 每次去尾
        for (int i = len - 1; i >= 0; i--) {
            ArrayUtil.swap(arr, 0, i);
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
            ArrayUtil.swap(arr, max, i);
            heapify(arr, max, len);
        }
    }
}
