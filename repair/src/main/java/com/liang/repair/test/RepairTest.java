package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 100; i++) {
            String s = String.format("  - { taskId: \"%s\", sourceName: \"491.prism_shareholder_path\", tableName: \"ratio_path_company_new_%s\", columns: \"*\", where: \"1=1\", scanMode: TumblingWindow }", i, i);
            System.out.println(s);
        }
    }
}
