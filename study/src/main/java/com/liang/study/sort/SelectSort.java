package com.liang.study.sort;

public class SelectSort implements ISort {
    public static void main(String[] args) {
        ArrayUtils.testSort(new SelectSort());
    }

    /**
     * 遍历[0, length), 最小值放在arr[0]
     * 遍历[1, length), 最小值放在arr[1]
     * 遍历[2, length), 最小值放在arr[2]
     * 遍历[3, length), 最小值放在arr[3]
     * 以此类推...
     */
    @Override
    public void sort(int[] arr) {
        int len = arr.length;
        for (int i = 0; i < len; i++) {
            int min = i;
            for (int j = i + 1; j < len; j++) {
                if (arr[min] > arr[j]) {
                    min = j;
                }
            }
            if (min != i) {
                ArrayUtils.swap(arr, min, i);
            }
        }
    }
}
