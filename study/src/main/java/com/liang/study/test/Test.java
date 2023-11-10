package com.liang.study.test;

import com.liang.study.sort.ArrayUtils;
import com.liang.study.sort.ISort;

public class Test {
    public static void main(String[] args) {
        ArrayUtils.testSort(new ISort() {
            @Override
            public void sort(int[] arr) {
                int length = arr.length;
                for (int i = 0; i < length; i++) {
                    int index = i;
                    for (int j = i + 1; j < length; j++) {
                        if (arr[j] <= arr[index]) {
                            index = j;
                        }
                    }
                    if (index != i) {
                        ArrayUtils.swap(arr, i, index);
                    }
                }
            }
        });
    }
}
