package com.liang.repair.impl;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CooperationPartnerRepair extends ConfigHolder {
    public static void main(String[] args) {
        String url = "ttps://www.tianyancha.com/human/1846791630-c5991315558".replaceAll("(.*?)(\\d+.*)", "$2");
        String humanGraphId = url.split("-c")[0];
        String companyGraphId = url.split("-c")[1];
        String queryHumanPidSql = String.format("select human_pid from company_human_relation where human_graph_id = %s and company_graph_id = %s",
                SqlUtils.formatValue(humanGraphId), SqlUtils.formatValue(companyGraphId));
        String humanPid = new JdbcTemplate("157.prism_boss").queryForObject(queryHumanPidSql, rs -> rs.getString(1));
        if (!TycUtils.isTycUniqueEntityId(humanPid)) {
            log.error("not find human_pid from sql: {}", queryHumanPidSql);
            return;
        }
        log.info("human_pid: {}", humanPid);
        log.info("select * from cooperation_partner where boss_human_pid = '{}'", humanPid);
    }
}
