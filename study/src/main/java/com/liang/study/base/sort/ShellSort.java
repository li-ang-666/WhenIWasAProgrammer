package com.liang.study.base.sort;

import cn.hutool.core.util.ArrayUtil;

import java.util.function.Consumer;

public class ShellSort implements Consumer<int[]> {
    public static void main(String[] args) {
        SortUtils.testSort(new ShellSort());
    }

    /**
     * 易错点: j -= gap
     */
    @Override
    public void accept(int[] arr) {
        int len = arr.length;
        int k = 3;
        for (int gap = len / k; gap >= 1; gap /= k) {
            for (int i = 0; i < len; i++) {
                for (int j = i; j - gap >= 0 && arr[j - gap] > arr[j]; j -= gap) {
                    ArrayUtil.swap(arr, j - gap, j);
                }
            }
        }
    }
}
