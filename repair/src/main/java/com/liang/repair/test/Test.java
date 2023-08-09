package com.liang.repair.test;

import com.liang.common.dto.tyc.Company;
import com.liang.repair.service.ConfigHolder;

public class Test extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        System.out.println(new Company().getGid());
    }
}
