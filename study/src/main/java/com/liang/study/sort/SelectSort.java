package com.liang.study.sort;

import org.junit.Test;

public class SelectSort {
    @Test
    public void test() {
        ArrayUtils.test(this::sort);
    }

    public void sort(int[] arr) {
        //冒泡排序背过, 选择排序靠理解写
        for (int i = 0; i <= arr.length - 2; i++) {
            int min = i;
            for (int j = i + 1; j <= arr.length - 1; j++) {
                if (arr[j] < arr[min])
                    min = j;
            }
            if (min != i) {
                ArrayUtils.swap(arr, i, min);
            }
        }
    }
}
