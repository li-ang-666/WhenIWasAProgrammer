package com.liang.study.base.sort;

import cn.hutool.core.util.ArrayUtil;

import java.util.function.Consumer;

public class BubbleSort implements Consumer<int[]> {
    public static void main(String[] args) {
        SortUtils.testSort(new BubbleSort());
    }

    /**
     * 内外圈相似, 背过!!!!!
     * 外圈遍历的含义是已经完成了i个
     * 内圈每次都从0开始冒泡(角标最大值是j+1, 所以要求j+1 < len-i)
     * 相等时[不]交换, 则为稳定排序
     */
    @Override
    public void accept(int[] arr) {
        int len = arr.length;
        for (int i = 0; i < len; i++) {
            boolean changed = false;
            for (int j = 0; j < len - i - 1; j++) {
                if (arr[j] > arr[j + 1]) {
                    ArrayUtil.swap(arr, j, j + 1);
                    changed = true;
                }
            }
            if (!changed) {
                return;
            }
        }
    }
}
