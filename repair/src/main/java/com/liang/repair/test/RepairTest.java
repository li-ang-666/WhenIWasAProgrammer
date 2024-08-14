package com.liang.repair.test;

import cn.hutool.core.util.StrUtil;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        JdbcTemplate jdbcTemplate = new JdbcTemplate("436.judicial_risk");
        for (int i = 1; i <= 32; i++) {
            String num = String.valueOf(i);
            String table = "company_lawsuit_struc_" + StrUtil.fillBefore(num, '0', 3);
            String sql = new SQL().SELECT("1")
                    .FROM(table)
                    .WHERE("company_lawsuit_uuid = 'f064129fdc8e41508886dc7ed79d386c'")
                    .toString();
            String res = jdbcTemplate.queryForObject(sql, rs -> rs.getString(1));
            System.out.println(i + " -> " + res);
        }
    }
}
