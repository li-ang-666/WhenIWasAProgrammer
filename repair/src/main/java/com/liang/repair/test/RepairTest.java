package com.liang.repair.test;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.util.stream.Collectors;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        String source = "test_db.ads_user_tag_commercial_df_tmp";
        String sink = "bak." + source.split("\\.")[1];
        JdbcTemplate doris = new JdbcTemplate("doris");
        // drop sink
        String dropSql = String.format("drop table if exists %s force", sink);
        doris.update(dropSql);
        // write sink
        String descSql = "desc " + source;
        String createSql = doris.queryForList(descSql, rs -> rs.getString(1))
                .stream()
                .map(e -> String.format("cast(%s as string) %s", e, e))
                .collect(Collectors.joining(", ", String.format("create table %s as select ", sink), String.format(" from %s", source)));
        doris.update(createSql);
    }
}
