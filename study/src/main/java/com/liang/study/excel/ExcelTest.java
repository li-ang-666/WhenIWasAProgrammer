package com.liang.study.excel;

import cn.hutool.poi.excel.ExcelUtil;
import cn.hutool.poi.excel.ExcelWriter;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class ExcelTest {
    public static void main(String[] args) throws Exception {
        Map<String, Integer> companyToLevel = new HashMap<>();
        ExcelUtil.getReader(new File("/Users/liang/Downloads/长城-苏州-0110(1)/苏州_渠道公司.xlsx"))
                .readAll(Map.class)
                .forEach(columnMap -> {
                    String companyName = String.valueOf(columnMap.get("中标单位"));
                    int num = Integer.parseInt(String.valueOf(columnMap.get("合作的业主数量")));
                    int level;
                    if (6 <= num && num <= 10) {
                        level = 1;
                    } else if (11 <= num && num <= 20) {
                        level = 2;
                    } else if (20 < num) {
                        level = 3;
                    } else if (1 <= num && num <= 5) {
                        level = 4;
                    } else if (num == 0) {
                        level = 5;
                    } else {
                        level = 5;
                    }
                    companyToLevel.putIfAbsent(companyName, level);
                });
        List<Map<String, Object>> companyWithLevel = ExcelUtil.getReader(new File("/Users/liang/Downloads/长城-苏州-0110(1)/苏州_联系人.xlsx"))
                .readAll(Map.class)
                .stream()
                .map(e -> {
                    HashMap<String, Object> columnMap = new HashMap<>();
                    e.forEach((k, v) -> columnMap.put(String.valueOf(k), v));
                    String companyName = String.valueOf(columnMap.get("渠道名称"));
                    Integer level = companyToLevel.getOrDefault(companyName, 0);
                    columnMap.put("渠道评级", level);
                    return columnMap;
                }).collect(Collectors.toList());
        List<Map<String, Object>> companyGroupA = companyWithLevel.stream().filter(e -> {
            int level = Integer.parseInt(String.valueOf(e.get("渠道评级")));
            return level == 1 || level == 2;
        }).sorted(Comparator.comparing(e -> String.valueOf(e.get("渠道名称")))).collect(Collectors.toList());
        List<Map<String, Object>> companyGroupB = companyWithLevel.stream().filter(e -> {
            int level = Integer.parseInt(String.valueOf(e.get("渠道评级")));
            return level == 3 || level == 4;
        }).sorted(Comparator.comparing(e -> String.valueOf(e.get("渠道名称")))).collect(Collectors.toList());
        List<Map<String, Object>> companyGroupC = companyWithLevel.stream().filter(e -> {
            int level = Integer.parseInt(String.valueOf(e.get("渠道评级")));
            return level != 1 && level != 2 && level != 3 && level != 4;
        }).sorted(Comparator.comparing(e -> String.valueOf(e.get("渠道名称")))).collect(Collectors.toList());
        Map<String, Double> managerToRatio = new HashMap<String, Double>() {{
            put("甲", 0.3);
            put("乙", 0.3);
            put("丙", 0.3);
            put("丁", 0.1);
        }};
        parseGroup(companyGroupA, managerToRatio);
    }

    private static void parseGroup(List<Map<String, Object>> columnMaps, Map<String, Double> ratio) {
        Queue<Map.Entry<String, Double>> queue = new ArrayDeque<>();
        for (Map.Entry<String, Double> entry : ratio.entrySet()) {
            entry.setValue(columnMaps.size() * entry.getValue());
            queue.offer(entry);
        }
        System.out.println(queue);
        String lastCompany = String.valueOf(columnMaps.get(0).get("渠道名称"));
        Map.Entry<String, Double> manager = queue.poll();
        for (Map<String, Object> columnMap : columnMaps) {
            double left = manager.getValue() - 1;
            String thisCompany = String.valueOf(columnMap.get("渠道名称"));
            if (left > 0 || thisCompany.equals(lastCompany) || queue.isEmpty()) {
                manager.setValue(left);
            } else {
                manager = queue.poll();
                manager.setValue(manager.getValue() - 1);
            }
            lastCompany = thisCompany;
            columnMap.put("负责人", String.valueOf(manager.getKey()));
        }
        ExcelWriter writer = ExcelUtil.getWriter(new File("/Users/liang/Desktop/aaa.xlsx"));
        writer.write(columnMaps);
        writer.flush();
        //columnMaps.forEach(e -> System.out.println(e));
    }
}
