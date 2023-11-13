package com.liang.study.base.search;

public class BinarySearch {
    public static void main(String[] args) {
        System.out.println(find(new int[]{1, 3, 5, 7, 9}, 1));
        System.out.println(find(new int[]{1, 3, 5, 7, 9}, 2));
        System.out.println(find(new int[]{1, 3, 5, 7, 9}, 3));
        System.out.println(find(new int[]{1, 3, 5, 7, 9}, 4));
        System.out.println(find(new int[]{1, 3, 5, 7, 9}, 5));
    }

    public static int find(int[] arr, int num) {
        int l = 0, r = arr.length - 1;
        while (l <= r) {
            int mid = (l + r) / 2;
            if (num == arr[mid]) {
                return mid;
            } else if (num < arr[mid]) {
                r = mid - 1;
            } else {
                l = mid + 1;
            }
        }
        //return l; //若是二分插入, 则返回l
        return -1;
    }
}

