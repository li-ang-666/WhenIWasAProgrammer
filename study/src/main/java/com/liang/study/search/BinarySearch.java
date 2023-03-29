package com.liang.study.search;

import org.junit.Test;

/**
 * https://leetcode.cn/problems/find-first-and-last-position-of-element-in-sorted-array/?favorite=2cktkvj
 */
public class BinarySearch {
    @Test
    public void test() {
        System.out.println(find(new int[]{1, 3, 5, 7, 9}, 1));
        System.out.println(find(new int[]{1, 3, 5, 7, 9}, 2));
        System.out.println(find(new int[]{1, 3, 5, 7, 9}, 3));
        System.out.println(find(new int[]{1, 3, 5, 7, 9}, 4));
        System.out.println(find(new int[]{1, 3, 5, 7, 9}, 5));
    }

    public int find(int[] arr, int i) {
        int l = 0, r = arr.length - 1;
        while (l <= r) {
            int mid = (l + r) / 2;
            if (i == arr[mid]) {
                return mid;
            } else if (i < arr[mid]) {
                r = mid - 1;
            } else {
                l = mid + 1;
            }
        }
        //return l; //若是二分插入, 则返回l
        return -1;
    }
}

