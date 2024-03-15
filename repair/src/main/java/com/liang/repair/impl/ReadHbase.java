package com.liang.repair.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.repair.service.ConfigHolder;

public class ReadHbase extends ConfigHolder {
    private final static HbaseTemplate HBASE_TEMPLATE;

    static {
        HBASE_TEMPLATE = new HbaseTemplate("hbaseSink");
    }

    public static void main(String[] args) {
        HbaseOneRow hbaseOneRow = new HbaseOneRow(HbaseSchema.COMPANY_ALL_COUNT, "1623967788");
        HbaseOneRow queryResult = query(hbaseOneRow);
        //queryResult.put("num_control_ability", null);
        //queryResult.put("num_benefit_ability", null);
        //queryResult.put("has_beneficiary", null);
        //queryResult.put("has_controller", null);
        //update(queryResult);
    }

    private static HbaseOneRow query(HbaseOneRow hbaseOneRow) {
        HbaseOneRow resultRow = HBASE_TEMPLATE.getRow(hbaseOneRow);
        int hbaseSinkConfigLength = String.valueOf(ConfigUtils.getConfig().getHbaseConfigs().get("hbaseSink")).split(",").length;
        if (hbaseSinkConfigLength == 1) log.warn("\n\n醒目: 目前是测试Hbase\n");
        else log.warn("\n\n醒目: 目前是生产Hbase\n");
        log.info("row: {}", JsonUtils.toString(resultRow));
        return resultRow;
    }

    private static void update(HbaseOneRow hbaseOneRow) {
        HBASE_TEMPLATE.update(hbaseOneRow);
    }
}
