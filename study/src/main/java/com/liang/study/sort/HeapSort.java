package com.liang.study.sort;

public class HeapSort implements ISort {
    public static void main(String[] args) {
        ArrayUtils.testSort(new HeapSort());
    }

    @Override
    public void sort(int[] arr) {
        int len = arr.length;
        for (int i = (len - 2) / 2; i >= 0; i--) {
            heapify(arr, i, len);
        }
        for (int i = len - 1; i >= 0; i--) {
            ArrayUtils.swap(arr, i, 0);
            heapify(arr, 0, i);
        }
    }

    private void heapify(int[] arr, int idx, int len) {
        int max = idx;
        int l = idx * 2 + 1, r = idx * 2 + 2;
        if (l < len && arr[max] < arr[l]) {
            max = l;
        }
        if (r < len && arr[max] < arr[r]) {
            max = r;
        }
        if (max != idx) {
            ArrayUtils.swap(arr, max, idx);
            heapify(arr, max, len);
        }
    }
}
