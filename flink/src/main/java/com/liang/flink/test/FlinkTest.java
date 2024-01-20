package com.liang.flink.test;

import cn.hutool.core.io.IoUtil;
import com.liang.common.util.DorisBitmapUtils;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

@Slf4j
public class FlinkTest {
    private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String URL = "jdbc:hive2://10.99.202.153:2181,10.99.198.86:2181,10.99.203.51:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
    private static final String USER = "hive";
    private static final String PASSWORD = "";

    public static void main(String[] args) throws Exception {
        Class.forName(DRIVER);
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            ResultSet rs = connection.prepareStatement("select id, bitmap from test.bitmap_test").executeQuery();
            while (rs.next()) {
                System.out.println("id: " + rs.getString(1));
                Roaring64NavigableMap bitmap = DorisBitmapUtils.parseBinary(IoUtil.readBytes(rs.getBinaryStream(2)));
                System.out.println("min: " + bitmap.stream().min().getAsLong());
                System.out.println("max: " + bitmap.stream().max().getAsLong());
            }
        }
    }
}
