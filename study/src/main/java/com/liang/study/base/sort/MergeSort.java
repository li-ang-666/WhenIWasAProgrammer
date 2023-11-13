package com.liang.study.base.sort;

import java.util.function.Consumer;

public class MergeSort implements Consumer<int[]> {
    public static void main(String[] args) {
        SortUtils.testSort(new MergeSort());
    }

    @Override
    public void accept(int[] arr) {
        mergeSort(arr, 0, arr.length - 1, new int[arr.length]);
    }

    /**
     * arr[p1]<=arr[p2] 时是稳定排序
     */
    private void mergeSort(int[] arr, final int l, final int r, final int[] tmp) {
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
