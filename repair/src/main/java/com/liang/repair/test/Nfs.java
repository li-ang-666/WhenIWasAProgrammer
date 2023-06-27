package com.liang.repair.test;

public class Nfs {
    public static void main(String[] args) {
        String pt = "20230618";
        String orgId = "wangzhitianyuanT7";
        String tableName = "company_tm";

        String fromDir = String.format("/nfs/open_data/dblog/%s/all_table/%s/_c1=%s", pt, pt, tableName);
        String toDir = String.format("/nfs/ftp/databases3/%s/%s/_c1=%s", orgId, pt, tableName);

        System.out.println(String.format("rm -rf %s/* ", toDir));
        System.out.println(String.format("cp -r %s/* %s/ ", fromDir, toDir));
    }
}
