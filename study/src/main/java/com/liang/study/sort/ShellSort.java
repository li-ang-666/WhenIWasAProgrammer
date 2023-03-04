package com.liang.study.sort;

import org.junit.Test;

public class ShellSort {
    @Test
    public void test() {
        ArrayUtils.test(this::sort);
    }

    public void sort(int[] arr) {
        int n = arr.length;
        for (int incr = n / 2; incr >= 1; incr /= 2) {
            for (int i = incr; i <= n - 1; i++) {
                //直接插入排序的incr恒等于1而已
                for (int j = i; j - incr >= 0 && arr[j - incr] > arr[j]; j--) {
                    ArrayUtils.swap(arr, j - incr, j);
                }
            }
        }
    }
}
