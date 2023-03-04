package com.liang.repair.impl;

import com.liang.common.database.template.JdbcTemplate;
import com.liang.common.util.GlobalUtils;
import com.liang.repair.trait.AbstractRunner;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.List;

public class Search extends AbstractRunner {
    @Override
    public void run(String[] args) throws Exception {
        ParameterTool parameterTool = GlobalUtils.getParameterTool();
        JdbcTemplate jdbcTemplate = jdbc("hcmproduction");
        List<String> employees = jdbcTemplate.queryForList("select distinct employee_id from hcm_hr_job_change where ent_id = 136 and create_time >= '2022-01-01' and effect_date >= '2022-01-01'", rs -> rs.getString(1));
        for (String employee : employees) {
            String sql = String.format("select employee_id,group_concat(org_tree order by effect_date,create_time) from hcm_hr_job_change where employee_id = %s group by employee_id", employee);
            String res = jdbcTemplate.queryForObject(sql, rs -> {
                String before = rs.getString(1);
                String after = rs.getString(2);
                if (!(after != null && (after.startsWith("技术平台部") || after.startsWith("People产品线") || after.startsWith("ATS产品线")))) {
                    return rs.getString(1) + " | " + rs.getString(2);
                }
                return null;
            });
            log.info(res);
        }
    }
}
