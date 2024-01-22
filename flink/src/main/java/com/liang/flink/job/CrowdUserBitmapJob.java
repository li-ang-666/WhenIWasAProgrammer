package com.liang.flink.job;

import cn.hutool.core.io.IoUtil;
import com.liang.common.dto.Config;
import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.service.database.template.DorisWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.DorisBitmapUtils;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;

@Slf4j
public class CrowdUserBitmapJob {
    private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String URL = "jdbc:hive2://10.99.202.153:2181,10.99.198.86:2181,10.99.203.51:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
    private static final String USER = "hive";
    private static final String PASSWORD = "";
    private static final DorisSchema DORIS_SCHEMA = DorisSchema.builder()
            .database("test").tableName("bitmap_test")
            .derivedColumns(Collections.singletonList("user_id_bitmap = if(user_id < 1, bitmap_empty(), to_bitmap(user_id))"))
            .build();

    public static void main(String[] args) throws Exception {
        Config config = ConfigUtils.createConfig("");
        ConfigUtils.setConfig(config);
        // init dorisWriter
        DorisWriter dorisWriter = new DorisWriter("dorisSink", 256 * 1024 * 1024);
        // jdbc query
        Class.forName(DRIVER);
        Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
        connection.prepareStatement("set spark.executor.memory=10g").executeUpdate();
        PreparedStatement preparedStatement = connection.prepareStatement("select id, bitmap from test.bitmap_test where id = 'company_bond_plates'");
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            String id = resultSet.getString(1);
            InputStream bitmapInputStream = resultSet.getBinaryStream(2);
            Roaring64NavigableMap bitmap = DorisBitmapUtils.parseBinary(IoUtil.readBytes(bitmapInputStream));
            if (bitmap.isEmpty()) {
                DorisOneRow dorisOneRow = new DorisOneRow(DORIS_SCHEMA);
                dorisOneRow.put("id", id);
                dorisOneRow.put("user_id", 0);
                dorisWriter.write(dorisOneRow);
                continue;
            }
            bitmap.forEach(userId -> {
                DorisOneRow dorisOneRow = new DorisOneRow(DORIS_SCHEMA);
                dorisOneRow.put("id", id);
                dorisOneRow.put("user_id", userId);
                dorisWriter.write(dorisOneRow);
            });
        }
        // flush the memory cache
        dorisWriter.flush();
        // close
        resultSet.close();
        preparedStatement.close();
        connection.close();
    }
}
