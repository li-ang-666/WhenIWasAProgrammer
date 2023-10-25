package com.liang.flink.test;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

@Slf4j
public class FlinkTest {
    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://e1d4c0a1d8d1456ba4b461ab8b9f293din01.internal.cn-north-4.mysql.rds.myhuaweicloud.com:3306/prism_shareholder_path", "jdhw_d_data_dml", "2s0^tFa4SLrp72");
        ResultSet rs = connection.prepareStatement("show tables").executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
}