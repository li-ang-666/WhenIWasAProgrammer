package com.liang.flink.basic;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import lombok.SneakyThrows;

public class SshTunnel {
    @SneakyThrows
    public static void open() {
        JSch jSch = new JSch();
        Session session = jSch.getSession("liang", "10.99.194.108", 22);
        session.setPassword("Moka20190520");
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect();
        session.setPortForwardingL(3306, "36c607bfd9174d4e81512aa73375f0fain01.internal.cn-north-4.mysql.rds.myhuaweicloud.com", 3306);
    }
}
