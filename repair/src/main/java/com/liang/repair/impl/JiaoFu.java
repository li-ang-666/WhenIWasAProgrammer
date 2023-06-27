package com.liang.repair.impl;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * ll /nfs/ftp/databases3
 */
public class JiaoFu {
    public static void main(String[] args) {
        /**
         * 辛苦老师们优先解决下一下客户的问题：
         * 网智天元客户是数据库产品，FTP的，反馈：
         * 日更文件格式不对的问题：
         * 20230616，company_patent、company_tm
         * 20230617，company、company_patent、company_tm
         * 20230618，company_tm
         * 视野金融的
         * 20230619， company_patent、compnay_tm
         * 20230617，company_paten
         * 20230616, company_patent
         * 20230615, company_patent
         */
        printShell(new Target("20230616", "wangzhitianyuanT7", "company_patent"));
        printShell(new Target("20230616", "wangzhitianyuanT7", "company_tm"));
        printShell(new Target("20230617", "wangzhitianyuanT7", "company"));
        printShell(new Target("20230617", "wangzhitianyuanT7", "company_patent"));
        printShell(new Target("20230617", "wangzhitianyuanT7", "company_tm"));
        printShell(new Target("20230618", "wangzhitianyuanT7", "company_tm"));

        printShell(new Target("20230619", "shiyejinfunew", "company_patent"));
        printShell(new Target("20230619", "shiyejinfunew", "company_tm"));
        printShell(new Target("20230617", "shiyejinfunew", "company_patent"));
        printShell(new Target("20230616", "shiyejinfunew", "company_patent"));
        printShell(new Target("20230615", "shiyejinfunew", "company_patent"));
    }

    private static void printShell(Target target) {
        String pt = target.pt;
        String orgId = target.orgId;
        String tableName = target.tableName;
        String fromDir = String.format("/nfs/open_data/dblog/%s/all_table/%s/_c1=%s", pt, pt, tableName);
        String toDir = String.format("/nfs/ftp/databases3/%s/%s/_c1=%s", orgId, pt, tableName);

        String head = String.format("# %s", target);
        System.out.println("echo '" + head + "'");
        String lsFromDir = String.format("# ls %s # fromDir", fromDir);
        System.out.println("echo '" + lsFromDir + "'");
        String lsToDir = String.format("# ls %s # toDir", toDir);
        System.out.println("echo '" + lsToDir + "'");

        String rm = String.format("rm -rf %s/*", toDir);
        System.out.println("echo '" + rm + "'");
        System.out.println(rm);
        String cp = String.format("cp -r %s/* %s/", fromDir, toDir);
        System.out.println("echo '" + cp + "'");
        System.out.println(cp);

        System.out.println();
    }

    @AllArgsConstructor
    @Data
    private static final class Target {
        private String pt;
        private String orgId;
        private String tableName;
    }
}
