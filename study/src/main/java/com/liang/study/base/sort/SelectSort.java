package com.liang.study.base.sort;

import cn.hutool.core.util.ArrayUtil;

import java.util.function.Consumer;

public class SelectSort implements Consumer<int[]> {
    public static void main(String[] args) {
        SortUtils.testSort(new SelectSort());
    }

    /**
     * 遍历[0, length), 最小值放在arr[0]
     * 遍历[1, length), 最小值放在arr[1]
     * 遍历[2, length), 最小值放在arr[2]
     * 遍历[3, length), 最小值放在arr[3]
     * 以此类推...
     */
    @Override
    public void accept(int[] arr) {
        int len = arr.length;
        for (int i = 0; i < len; i++) {
            int min = i;
            for (int j = i + 1; j < len; j++) {
                if (arr[min] > arr[j]) {
                    min = j;
                }
            }
            if (min != i) {
                ArrayUtil.swap(arr, min, i);
            }
        }
    }
}
