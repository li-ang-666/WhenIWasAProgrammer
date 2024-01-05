package com.liang.flink.test;

import cn.hutool.core.io.IoUtil;
import com.liang.flink.job.BigShareholderJob;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class FlinkTest {
    public static void main(String[] args) throws Exception {
        InputStream stream = BigShareholderJob.class.getClassLoader()
                .getResourceAsStream("equity_ratio_source_100_company.txt");
        String[] arr = IoUtil.read(stream, StandardCharsets.UTF_8)
                .trim()
                .split("\n");
        Set<String> set = Arrays.stream(arr)
                .filter(e -> e.matches(".*?(\\d+).*"))
                .map(e -> e.replaceAll(".*?(\\d+).*", "$1"))
                .collect(Collectors.toSet());
        System.out.println(set);
        System.out.println(set.size());
    }
}
