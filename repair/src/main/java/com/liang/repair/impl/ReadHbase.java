package com.liang.repair.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.repair.service.ConfigHolder;

public class ReadHbase extends ConfigHolder {
    public static void main(String[] args) {
        // 连接
        HbaseTemplate hbaseTemplate = new HbaseTemplate("hbaseSink");
        // 条件
        HbaseOneRow queryRow = new HbaseOneRow(HbaseSchema.HISTORICAL_INFO_SPLICE, "3481084136");
        // 查询
        HbaseOneRow resultRow = hbaseTemplate.getRow(queryRow);
        // 打印
        if (String.valueOf(ConfigUtils.getConfig().getHbaseDbConfigs().get("hbaseSink")).split(",").length == 1) {
            log.warn("\n\n醒目: 目前是测试Hbase\n");
        } else {
            log.warn("\n\n醒目: 目前是生产Hbase\n");
        }
        log.info("row: {}", JsonUtils.toString(resultRow));
        // 更正并写入
        hbaseTemplate.update(resultRow.put("history_court_announcement_defendant_subject_cnt", "11"));
    }
}
