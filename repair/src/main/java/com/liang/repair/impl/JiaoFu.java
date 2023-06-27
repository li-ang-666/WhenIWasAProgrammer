package com.liang.repair.impl;

import lombok.AllArgsConstructor;
import lombok.Data;

public class JiaoFu {
    public static void main(String[] args) {
        printShell(new Target("20230619", "wangzhitianyuanT7", "company_tm"));
    }

    private static void printShell(Target target) {
        String pt = target.pt;
        String orgId = target.orgId;
        String tableName = target.tableName;
        String fromDir = String.format("/nfs/open_data/dblog/%s/all_table/%s/_c1=%s", pt, pt, tableName);
        String toDir = String.format("/nfs/ftp/databases3/%s/%s/_c1=%s", orgId, pt, tableName);

        System.out.println(String.format("# %s", target));

        System.out.println(String.format("# ls %s # fromDir", fromDir));
        System.out.println(String.format("# ls %s # toDir", toDir));

        System.out.println(String.format("rm -rf %s/*", toDir));
        System.out.println(String.format("cp -r %s/* %s/", fromDir, toDir));
    }

    @AllArgsConstructor
    @Data
    private static final class Target {
        private String pt;
        private String orgId;
        private String tableName;
    }
}
