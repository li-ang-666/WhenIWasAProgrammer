package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.project.ratio.path.company.RatioPathCompanyService;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class RatioPathCompanyRepair {
    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig(null);
        ConfigUtils.setConfig(config);
        JdbcTemplate jdbcTemplate = new JdbcTemplate("457.prism_shareholder_path");
        Set<String> companyIds = new HashSet<>(Arrays.asList(
                "2318558208"
        ));
        Set<Long> allCompanyIds = companyIds.stream().flatMap(e -> {
            HashSet<Long> res = new HashSet<>();
            if (StringUtils.isNumeric(e)) {
                res.add(Long.parseLong(e));
            }
            String sql = new SQL().SELECT("distinct company_id")
                    .FROM("ratio_path_company")
                    .WHERE("shareholder_id = " + SqlUtils.formatValue(e))
                    .toString();
            jdbcTemplate.queryForList(sql, rs -> {
                String companyId = rs.getString(1);
                if (StringUtils.isNumeric(companyId)) {
                    res.add(Long.parseLong(companyId));
                }
                return null;
            });
            return res.stream();
        }).collect(Collectors.toSet());

        new RatioPathCompanyService().invoke(allCompanyIds);
        ConfigUtils.unloadAll();
    }
}
