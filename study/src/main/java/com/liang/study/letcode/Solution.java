package com.liang.study.letcode;

import java.util.ArrayList;
import java.util.Arrays;

public class Solution {
    public int[][] merge(int[][] intervals) {
        Arrays.sort(intervals, (e1, e2) -> e1[0] - e2[0]);

        for (int i = 1; i < intervals.length; i++) {
            if (intervals[i][0] <= intervals[i - 1][1]) {
                intervals[i][0] = intervals[i - 1][0];
                intervals[i][1] = Math.max(intervals[i - 1][1], intervals[i][1]);
            }
        }

        ArrayList<int[]> result = new ArrayList<>();
        for (int i = 0; i < intervals.length; i++) {
            if (i + 1 < intervals.length && intervals[i + 1][0] == intervals[i][0]) {
                continue;
            }
            result.add(intervals[i]);

        }
        return result.toArray(new int[][]{});
    }
}

