package com.liang.repair.impl.cache;

import com.liang.common.util.SqlUtils;
import com.liang.repair.service.ConfigHolder;

public class StringTest extends ConfigHolder {
    public static void main(String[] args) {
        System.out.println(SqlUtils.isCompanyId(""));
        System.out.println(SqlUtils.isCompanyId("null"));
        System.out.println(SqlUtils.isCompanyId("Null"));
        System.out.println(SqlUtils.isCompanyId("NULL"));
        System.out.println(SqlUtils.isCompanyId("0"));
        System.out.println("--------------------------");
        System.out.println(SqlUtils.isShareholderId("94jfrnfnuef"));
        System.out.println(SqlUtils.isShareholderId(""));
        System.out.println(SqlUtils.isShareholderId("0"));
        System.out.println(SqlUtils.isShareholderId("null"));
        System.out.println(SqlUtils.isShareholderId("NULL"));
        System.out.println(SqlUtils.isShareholderId("Null"));
        System.out.println(SqlUtils.isShareholderId("S011DMN09MG24PND6"));
    }
}
