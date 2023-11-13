package com.liang.study.base.sort;

import lombok.experimental.UtilityClass;

import java.util.Arrays;
import java.util.function.Consumer;

@UtilityClass
public class SortUtils {

    private final static int[][] arrays = {
            new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9},
            new int[]{9, 8, 7, 6, 5, 4, 3, 2, 1},
            new int[]{9, 8, 7, 3, 2, 1, 6, 5, 4},
            new int[]{8, 6, 4, 2, 1, 3, 5, 7, 9},
            new int[]{8, 7, 9, 2, 1, 3, 5, 4, 6},
            new int[]{1, 1, 1, 2, 2, 2, 3, 3, 3},
            new int[]{3, 3, 3, 2, 2, 2, 1, 1, 1},
            new int[]{1, 2, 3, 1, 2, 3, 1, 2, 3},
            new int[]{3, 2, 1, 3, 2, 1, 3, 2, 1},
            new int[]{2, 3, 1, 2, 1, 3, 2, 1, 3},
    };

    public static void testSort(Consumer<int[]> sorter) {
        for (int[] arr : arrays) {
            sorter.accept(arr);
            String my = Arrays.toString(arr);
            Arrays.sort(arr);
            String official = Arrays.toString(arr);
            System.out.println(my + " " + my.equals(official));
        }
    }
}
