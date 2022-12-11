package com.liang.study.sort;

public class QuickSort {
    public static void main(String[] args) {
        int[] arr = {3, 3, 3, 3, 1, 2, 4};
        quickSort(arr);
        for (int i : arr) {
            System.out.println(i);
        }
    }

    private static void quickSort(int[] a) {
        quickSort(a, 0, a.length - 1);
    }

    private static void quickSort(int[] a, int l, int r) {
        if (!(l < r)) return;
        int pivot = l, i = l + 1, j = r;
        while (true) {
            while (i <= r && a[i] < a[pivot]) i++;
            while (j > l && a[j] > a[pivot]) j--;
            if (i > j) break;
            int t = a[i];
            a[i] = a[j];
            a[j] = t;
            i++;
            j--;
        }
        int t = a[pivot];
        a[pivot] = a[j];
        a[j] = t;
        pivot = j;
        quickSort(a, l, pivot - 1);
        quickSort(a, pivot + 1, r);
    }
}
