package com.liang.study.sort;

import java.util.Arrays;

@FunctionalInterface
interface ISort {
    void sort(int[] arr);
}

public class ArrayUtils {
    public static void swap(int[] arr, int i, int j) {
        int temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    public static void test(ISort iSort) {
        int[][] arrs = {
                new int[]{9, 8, 7, 6, 5, 4, 3, 2, 1},
                new int[]{8, 7, 9, 2, 1, 3, 5, 4, 6}
        };
        for (int[] arr : arrs) {
            iSort.sort(arr);
            System.out.println(Arrays.toString(arr));
        }
    }
}

