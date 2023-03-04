package com.liang.study.sort;

import org.junit.Test;

public class QuickSort {
    @Test
    public void test() {
        ArrayUtils.test(this::sort);
    }

    public void sort(int[] arr) {
        sort(arr, 0, arr.length - 1);
    }

    /**
     * 快速排序是
     * 1、跳出
     * 2、p1=... p2=... pivot=... 复杂计算
     * 3、递归左边
     * 4、递归右边
     */
    public void sort(int[] arr, int start, int end) {
        if (start >= end) {
            return;
        }
        int p1 = start, p2 = end, pivot = arr[start];
        while (p1 < p2) {
            //右指针向内移动, 直到重合或者找一个比他小的
            //易错点, 左边为pivot, 右指针先动
            while (p1 < p2 && arr[p2] >= pivot)
                p2--;
            //左指针向内移动, 直到重合或者找一个比他大的
            while (p1 < p2 && arr[p1] <= pivot)
                p1++;
            //两个指针停止的时候, 只有2种情况
            //1、没重合, 交换两个指针指向的值
            //2、重合, 交换基准值和指针指向的值
            if (p1 < p2)
                ArrayUtils.swap(arr, p1, p2);
            else
                ArrayUtils.swap(arr, start, p1);

        }
        sort(arr, start, p1 - 1);
        sort(arr, p1 + 1, end);
    }
}
