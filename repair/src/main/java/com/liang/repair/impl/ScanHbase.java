package com.liang.repair.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.JsonUtils;
import com.liang.repair.service.ConfigHolder;

import java.util.*;

public class ScanHbase extends ConfigHolder {
    public static void main(String[] args) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate("bdpEquity");
        HbaseTemplate hbaseTemplate = new HbaseTemplate("hbaseSink");
        Map<String, Object> map = new HashMap<>();
        HbaseSchema hbaseSchema = HbaseSchema.builder()
                .namespace("prism_c")
                .tableName("human_all_count")
                .columnFamily("cf")
                .rowKeyReverse(false)
                .build();
        List<String> companyIdList = jdbcTemplate.queryForList("select tyc_unique_entity_id_beneficiary from entity_beneficiary_details where tyc_unique_entity_id_beneficiary >'1' limit 1000", rs -> rs.getString(1));
        HashSet<String> companyIdSet = new HashSet<>(companyIdList);
        for (String companyId : companyIdSet) {
            HbaseOneRow hbaseOneRow = new HbaseOneRow(hbaseSchema, companyId);
            Object count = hbaseTemplate.getRow(hbaseOneRow).getColumnMap().get("entity_beneficiary_details_count");
            if (count != null) {
                map.put(companyId, count);
            }
        }
        LinkedHashMap<String, Object> resultMap = new LinkedHashMap<>();
        map.entrySet().stream().sorted((e1, e2) -> {
            long num1 = Long.parseLong(String.valueOf(e1.getValue()));
            long num2 = Long.parseLong(String.valueOf(e2.getValue()));
            return (int) (num2 - num1);
        }).forEach(e -> {
            resultMap.put(e.getKey(), e.getValue());
        });

        System.out.println(JsonUtils.toString(resultMap));
    }
}
