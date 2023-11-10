package com.liang.study.sort;

import org.junit.Test;

public class BubbleSort {
    @Test
    public void test() {
        ArrayUtils.testSort(this::sort);
    }

    public void sort(int[] arr) {
        //两行for循环很相似, 直接背过
        //第一行的意思是, 我已经排好了多少个
        //第二行的意思是, 因为是冒泡到尾巴, 所以数组每次都去尾, 变短
        for (int i = 0; i < arr.length - 1; i++) {
            boolean changed = false;
            for (int j = 0; j < arr.length - 1 - i; j++) {
                if (arr[j] > arr[j + 1]) {
                    ArrayUtils.swap(arr, j, j + 1);
                    changed = true;
                }
            }
            if (!changed) {
                break;
            }
        }
    }
}
