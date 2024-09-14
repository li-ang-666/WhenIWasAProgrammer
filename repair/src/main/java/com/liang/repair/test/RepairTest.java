package com.liang.repair.test;

import com.liang.common.util.SqlUtils;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        System.out.println(SqlUtils.onDuplicateKeyUpdate("id", "name", "age"));
    }
}
