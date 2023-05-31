package com.liang.study.sort;

import org.junit.Test;

import static com.liang.study.sort.ArrayUtils.swap;

public class DailyTest {
    @Test
    public void test() {
        ArrayUtils.test(this::sort);
    }

    public void sort(int[] arr) {
        int n = arr.length;
        for (int incr = n / 2; incr >= 1; incr /= 2) {
            for (int i = incr; i <= n - 1; i++) {
                for (int j = i; j - incr >= 0 && arr[j - incr] > arr[j]; j -= incr) {
                    swap(arr, j, j - incr);
                }
            }
        }
    }
}
