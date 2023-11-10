package com.liang.study.test;

import com.liang.study.sort.ArrayUtils;
import com.liang.study.sort.ISort;

public class Test {
    public static void main(String[] args) {
        ArrayUtils.testSort(new ISort() {
            @Override
            public void sort(int[] arr) {

            }
        });
    }
}
