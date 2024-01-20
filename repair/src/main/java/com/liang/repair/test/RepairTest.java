package com.liang.repair.test;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        String[] sources = new String[]{
                "dwd.dwd_user_register_details",
                "dwd.dwd_coupon_info",
                "dwd.dwd_app_active",
                "dim.dim_user_comparison",
                "dwd.dwd_pay_point_com_detail",
                "dwd.dwd_dispatch_task",
                "dwd.dwd_basic_data_collect_monitor_hours",
                "ads.ads_user_tag"
        };
        for (String source : sources) {
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
            // check
            String sourceCount = doris.queryForObject("select count(1) from " + source, rs -> rs.getString(1));
            String sinkCount = doris.queryForObject("select count(1) from " + sink, rs -> rs.getString(1));
            while (Math.abs(Long.parseLong(sourceCount) - Long.parseLong(sinkCount)) > 100_000) {
                log.warn("sourceCnt {} not like sinkCnt {}", sourceCount, sinkCount);
                LockSupport.parkUntil(System.currentTimeMillis() + 1000 * 5);
                sourceCount = doris.queryForObject("select count(1) from " + source, rs -> rs.getString(1));
                sinkCount = doris.queryForObject("select count(1) from " + sink, rs -> rs.getString(1));
            }
            log.info("success!");
            log.info("sourceCnt {}", sourceCount);
            log.info("sinkCnt {}", sinkCount);
        }
    }
}
