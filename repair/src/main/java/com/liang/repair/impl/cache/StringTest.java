package com.liang.repair.impl.cache;

import com.liang.common.util.TycUtils;
import com.liang.repair.service.ConfigHolder;

public class StringTest extends ConfigHolder {
    public static void main(String[] args) {
        System.out.println(TycUtils.isCompanyId(""));
        System.out.println(TycUtils.isCompanyId("null"));
        System.out.println(TycUtils.isCompanyId("Null"));
        System.out.println(TycUtils.isCompanyId("NULL"));
        System.out.println(TycUtils.isCompanyId("0"));
        System.out.println("--------------------------");
        System.out.println(TycUtils.isShareholderId("94jfrnfnuef"));
        System.out.println(TycUtils.isShareholderId(""));
        System.out.println(TycUtils.isShareholderId("0"));
        System.out.println(TycUtils.isShareholderId("null"));
        System.out.println(TycUtils.isShareholderId("NULL"));
        System.out.println(TycUtils.isShareholderId("Null"));
        System.out.println(TycUtils.isShareholderId("S011DMN09MG24PND6"));
    }
}
