package com.liang.study.base.sort;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.Collectors;

public class SortTest {
    @Test
    public void testSort() {
        SortUtils.testSort(this::sort);
    }

    private void sort(int[] arr) {
        new ArrayList<String>().stream().filter(Objects::nonNull)
                .collect(Collectors.joining());
    }
}
