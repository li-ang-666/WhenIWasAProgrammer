package com.liang.study.letcode;

public class Solution {
    public int[] searchRange(int[] nums, int target) {
        if (nums.length == 0) {
            return new int[]{-1, -1};
        }
        int f = binarySearch(nums, target);
        if (f == -1) {
            return new int[]{-1, -1};
        }
        int l = -1;
        for (int i = f; i >= 0; i--) {
            if (nums[i] == target)
                l = i;
            else
                break;
        }
        int r = -1;
        for (int i = f; i <= nums.length - 1; i++) {
            if (nums[i] == target)
                r = i;
            else
                break;
        }
        return new int[]{l, r};
    }

    public int binarySearch(int[] arr, int t) {
        int l = 0;
        int r = arr.length - 1;
        while (l <= r) {
            int mid = (l + r) / 2;
            if (arr[mid] == t) {
                return mid;
            } else if (arr[mid] < t) {
                l = mid + 1;
            } else {
                r = mid - 1;
            }
        }
        return -1;
    }
}
