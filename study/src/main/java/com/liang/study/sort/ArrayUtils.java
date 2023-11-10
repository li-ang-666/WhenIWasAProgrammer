package com.liang.study.sort;

import java.util.Arrays;

public class ArrayUtils {
    public static void swap(int[] arr, int index1, int index2) {
        int temp = arr[index1];
        arr[index1] = arr[index2];
        arr[index2] = temp;
    }

    public static void testSort(ISort iSort) {
        int[][] arrays = {
                new int[]{9, 8, 7, 6, 5, 4, 3, 2, 1},
                new int[]{8, 7, 9, 2, 1, 3, 5, 4, 6}
        };
        for (int[] arr : arrays) {
            iSort.sort(arr);
            System.out.println(Arrays.toString(arr));
        }
    }
}

