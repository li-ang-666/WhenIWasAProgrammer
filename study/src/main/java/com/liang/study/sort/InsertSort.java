package com.liang.study.sort;

public class InsertSort implements ISort {
    public static void main(String[] args) {
        ArrayUtils.testSort(new InsertSort());
    }

    /**
     * 相等时不交换, 则为稳定排序
     */
    @Override
    public void sort(int[] arr) {
        int len = arr.length;
        for (int i = 0; i < len; i++) {
            for (int j = i; j - 1 >= 0 && arr[j - 1] > arr[j]; j--) {
                ArrayUtils.swap(arr, j - 1, j);
            }
        }
    }
}
