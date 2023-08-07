package com.liang.repair.impl.cache;

import com.liang.common.util.TycUtils;
import com.liang.repair.service.ConfigHolder;

public class StringTest extends ConfigHolder {
    public static void main(String[] args) {
        System.out.println(TycUtils.isUnsignedId(""));
        System.out.println(TycUtils.isUnsignedId("null"));
        System.out.println(TycUtils.isUnsignedId("Null"));
        System.out.println(TycUtils.isUnsignedId("NULL"));
        System.out.println(TycUtils.isUnsignedId("0"));
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
