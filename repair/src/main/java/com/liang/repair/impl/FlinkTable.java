package com.liang.repair.impl;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.liang.common.dto.Config;
import com.liang.common.service.database.factory.DruidFactory;
import com.liang.common.util.ConfigUtils;
import com.liang.repair.service.ConfigHolder;

import java.sql.ResultSet;

public class FlinkTable extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        Config config = ConfigUtils.getConfig();
        DruidDataSource pool = new DruidFactory().createPool("457.prism_shareholder_path");
        DruidPooledConnection connection = pool.getConnection();
        ResultSet resultSet = connection.prepareStatement("select * from ratio_path_company limit 1").executeQuery();
        resultSet.next();
        System.out.println(resultSet.getObject(1).getClass());
        System.out.println(resultSet.getObject(2).getClass());
        System.out.println(resultSet.getObject(3).getClass());
        System.out.println(resultSet.getObject(4).getClass());
        System.out.println(resultSet.getObject(5).getClass());
        System.out.println(resultSet.getObject(6).getClass());
        System.out.println(resultSet.getObject(7).getClass());
        System.out.println(resultSet.getObject(8).getClass());
        System.out.println(resultSet.getObject(9).getClass());
        System.out.println(resultSet.getObject(10).getClass());
        System.out.println(resultSet.getObject(11).getClass());
        System.out.println(resultSet.getObject(12).getClass());
        System.out.println(resultSet.getObject(13).getClass());
        System.out.println(resultSet.getObject(14).getClass());
    }
}
