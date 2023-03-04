package com.liang.study.sort;

import org.junit.Test;

public class MergeSort {
    @Test
    public void test() {
        ArrayUtils.test(this::sort);
    }

    public void sort(int[] arr) {
        sort(arr, new int[arr.length], 0, arr.length - 1);
    }

    /**
     * 归并排序是
     * 1、跳出
     * 2、递归左边
     * 3、递归右边
     * 4、p1=... p2=... p3=... 复杂计算
     */
    public void sort(int[] arr, int[] temp, int start, int end) {
        if (start >= end) {
            return;
        }
        int mid = (start + end) / 2;
        sort(arr, temp, start, mid);
        sort(arr, temp, mid + 1, end);
        int p1 = start, p2 = mid + 1, p3 = start;
        //p1, p2两个指针指向两个数组的头, 谁小谁入位, 都小左边的先
        while (p1 <= mid && p2 <= end) {
            if (arr[p1] <= arr[p2])
                temp[p3++] = arr[p1++];
            else
                temp[p3++] = arr[p2++];
        }
        //比较到最后, 总有一个数组已经都入位了, 另一个还差几个, 直接入位到尾部就好
        while (p1 <= mid) {
            temp[p3++] = arr[p1++];
        }
        while (p2 <= end) {
            temp[p3++] = arr[p2++];
        }
        //比较的时候写temp, 都入位后, 把temp的数据复制回原数组
        for (int i = start; i <= end; i++)
            arr[i] = temp[i];
    }
}
