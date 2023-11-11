package com.liang.study.sort;

public class MergeSort implements ISort {
    public static void main(String[] args) {
        ArrayUtils.testSort(new MergeSort());
    }

    /**
     * arr[p1]<=arr[p2] 时是稳定排序
     */
    @Override
    public void sort(int[] arr) {
        mergeSort(arr, 0, arr.length - 1, new int[arr.length]);
    }

    private void mergeSort(int[] arr, int l, int r, int[] tmp) {
        if (l >= r) {
            return;
        }
        int mid = (l + r) / 2;
        mergeSort(arr, l, mid, tmp);
        mergeSort(arr, mid + 1, r, tmp);
        int p1 = l, p2 = mid + 1, p3 = l;
        while (p1 <= mid && p2 <= r) {
            if (arr[p1] <= arr[p2]) {
                tmp[p3++] = arr[p1++];
            } else {
                tmp[p3++] = arr[p2++];
            }
        }
        while (p1 <= mid) {
            tmp[p3++] = arr[p1++];
        }
        while (p2 <= r) {
            tmp[p3++] = arr[p2++];
        }
        for (int i = l; i <= r; i++) {
            arr[i] = tmp[i];
        }
    }
}
