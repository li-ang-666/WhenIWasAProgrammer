package com.liang.repair.impl;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;

import java.util.Arrays;
import java.util.List;

public class Evaluate extends ConfigHolder {
    public static void main(String[] args) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate("150.data_index");
        List<String> ilEvaluationResultss = jdbcTemplate.queryForList("select distinct il_evaluation_results from zhixinginfo_evaluate_result_index", rs -> rs.getString(1));
        ilEvaluationResultss.stream()
                .filter(e -> String.valueOf(e).matches(".*?\\d+.*"))
                .flatMap(e -> Arrays.stream(e.split(";")))
                .distinct()
                .forEach(System.out::println);
    }
}
