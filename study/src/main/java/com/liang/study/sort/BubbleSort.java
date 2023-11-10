package com.liang.study.sort;

public class BubbleSort implements ISort {
    public static void main(String[] args) {
        ArrayUtils.testSort(new BubbleSort());
    }

    /**
     * 内外圈相似, 背过背过背过背过背过背过背过背过背过背过背过背过背过背过背过背过背过
     * 外圈遍历的含义是已经完成了i个
     * 内圈每次都从0开始冒泡(角标最大值是j+1, 所以要求j+1 < len-i)
     * 相等时不交换, 则为稳定排序
     */
    @Override
    public void sort(int[] arr) {
        int len = arr.length;
        for (int i = 0; i < len; i++) {
            boolean changed = false;
            for (int j = 0; j < len - i - 1; j++) {
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
