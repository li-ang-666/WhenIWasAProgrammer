package com.liang.repair.impl;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.util.zip.CRC32;

@Slf4j
public class CooperationPartnerRepair extends ConfigHolder {
    public static void main(String[] args) {
        // 2344020447-c3374246692
        String humanGraphId = "2344020447";
        String companyGraphId = "3374246692";
        String queryHumanPidSql = String.format("select human_pid from company_human_relation where human_graph_id = %s and company_graph_id = %s",
                SqlUtils.formatValue(humanGraphId), SqlUtils.formatValue(companyGraphId));
        String humanPid = new JdbcTemplate("157.prism_boss").queryForObject(queryHumanPidSql, rs -> rs.getString(1));
        if (!TycUtils.isTycUniqueEntityId(humanPid)) {
            log.error("not find human_pid from sql: {}", queryHumanPidSql);
            return;
        }
        CRC32 crc32 = new CRC32();
        crc32.update(humanPid.getBytes());
        long tableId = crc32.getValue() % 10;
        String queryJsonSql = String.format("select \n" +
                "  boss_human_gid human_name_id,\n" +
                "  boss_human_pid human_id,\n" +
                "  boss_human_name human_name,\n" +
                "  sum(cooperation_times_with_this_partner) total_cooperation_times,\n" +
                "  count(1) total_partners,\n" +
                "  concat('[', group_concat(json_object(\n" +
                "      \"hid\",             partner_human_gid,\n" +
                "      \"hpid\",            partner_human_pid,\n" +
                "      \"hname\",           partner_human_name,\n" +
                "      \"cid\",             company_gid,\n" +
                "      \"cname\",           company_name,\n" +
                "      \"cooperation_num\", cooperation_times_with_this_partner\n" +
                "  ) order by multi_cooperation_dense_rank), ']') neighbor\n" +
                "from (select * from cooperation_partner_%s where boss_human_pid = %s and single_cooperation_row_number = 1 order by multi_cooperation_dense_rank limit 0,100)t\n" +
                "group by boss_human_gid,boss_human_pid,boss_human_name", tableId, SqlUtils.formatValue(humanPid));
        String json = new JdbcTemplate("gauss").queryForObject(queryJsonSql, rs -> rs.getString(6));
        if (json == null) {
            log.error("not find json from sql: {}", queryJsonSql);
            return;
        }
        String updateSql = String.format("update human_pid_with_neighbor_list set neighbor = %s where human_id = %s", SqlUtils.formatValue(json), SqlUtils.formatValue(humanPid));
        new JdbcTemplate("040.human_base").update(updateSql);
    }
}
