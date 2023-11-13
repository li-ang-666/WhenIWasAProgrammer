package com.liang.study.base.sort;

import cn.hutool.core.util.ArrayUtil;

import java.util.function.Consumer;

public class QuickSort implements Consumer<int[]> {
    public static void main(String[] args) {
        SortUtils.testSort(new QuickSort());
    }

    @Override
    public void accept(int[] arr) {
        quickSort(arr, 0, arr.length - 1);
    }

    private void quickSort(int[] arr, final int l, final int r) {
        if (l >= r) {
            return;
        }
        int p1 = l, p2 = r, pivot = arr[l];
        // 易错点: 以最左为pivot, 需要从右指针开始动
        while (p1 < p2) {
            while (p1 < p2 && arr[p2] >= pivot) {
                p2--;
            }
            while (p1 < p2 && arr[p1] <= pivot) {
                p1++;
            }
            // 如果两个指针没有重合, 交换两个值
            if (p1 < p2) {
                ArrayUtil.swap(arr, p1, p2);
            }
            // 如果两个指针重合, 此时p1、p2的位置就是下次迭代的左右分界线, 先将pivot换到这里
            else {
                ArrayUtil.swap(arr, p1, l);
            }
        }
        //递归 分界线左边 与 分界线右边
        quickSort(arr, l, p1 - 1);
        quickSort(arr, p1 + 1, r);
    }
}
