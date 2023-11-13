package com.liang.study.base.sort;

import cn.hutool.core.util.ArrayUtil;

import java.util.function.Consumer;

public class InsertSort implements Consumer<int[]> {
    public static void main(String[] args) {
        SortUtils.testSort(new InsertSort());
    }

    /**
     * 相等时[不]交换, 则为稳定排序
     */
    @Override
    public void accept(int[] arr) {
        int len = arr.length;
        for (int i = 0; i < len; i++) {
            for (int j = i; j - 1 >= 0 && arr[j - 1] > arr[j]; j--) {
                ArrayUtil.swap(arr, j - 1, j);
            }
        }
    }
}
