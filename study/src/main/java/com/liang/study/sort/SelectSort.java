package com.liang.study.sort;

public class SelectSort implements ISort {
    public static void main(String[] args) {
        ArrayUtils.testSort(new SelectSort());
    }

    @Override
    public void sort(int[] arr) {
        int length = arr.length;
        for (int i = 0; i < length; i++) {
            int min = i;
            for (int j = i + 1; j < length; j++) {
                if (arr[j] < arr[min]) {
                    min = j;
                }
            }
            if (min != i) {
                ArrayUtils.swap(arr, min, i);
            }
        }
    }
}
