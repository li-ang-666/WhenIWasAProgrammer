package com.liang.repair.impl.cache;

import com.liang.common.util.TycStringUtils;
import com.liang.repair.service.ConfigHolder;

public class StringTest extends ConfigHolder {
    public static void main(String[] args) {
        System.out.println(TycStringUtils.isUnsignedId(""));
        System.out.println(TycStringUtils.isUnsignedId("null"));
        System.out.println(TycStringUtils.isUnsignedId("Null"));
        System.out.println(TycStringUtils.isUnsignedId("NULL"));
        System.out.println(TycStringUtils.isUnsignedId("0"));
        System.out.println("--------------------------");
        System.out.println(TycStringUtils.isShareholderId("94jfrnfnuef"));
        System.out.println(TycStringUtils.isShareholderId(""));
        System.out.println(TycStringUtils.isShareholderId("0"));
        System.out.println(TycStringUtils.isShareholderId("null"));
        System.out.println(TycStringUtils.isShareholderId("NULL"));
        System.out.println(TycStringUtils.isShareholderId("Null"));
        System.out.println(TycStringUtils.isShareholderId("S011DMN09MG24PND6"));
    }
}
