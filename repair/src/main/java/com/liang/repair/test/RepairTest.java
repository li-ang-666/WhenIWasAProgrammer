package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        for (int i = 1; i <= 99; i++) {
            System.out.println(String.format("alter table ratio_path_company_new_%s engine = innodb;", i));
        }
    }
}
