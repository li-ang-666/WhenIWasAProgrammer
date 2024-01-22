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
import java.sql.ResultSet;
import java.util.Collections;

/**
 * hive数据量:
 * select id, doris.bitmap_count(bitmap) cnt from test.bitmap_test;
 * +----------------------------------+------------+
 * |                id                |    cnt     |
 * +----------------------------------+------------+
 * | company_bond_plates              | 68840      |
 * | company_clean_info               | 371725684  |
 * | company_equity_relation_details  | 173317321  |
 * | company_human_relation           | 512669269  |
 * | company_index                    | 360007359  |
 * | company_legal_person             | 356816417  |
 * | personnel                        | 198732296  |
 * | senior_executive                 | 189916     |
 * | senior_executive_hk              | 375263     |
 * +----------------------------------+------------+
 */
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
        ResultSet resultSet = connection.prepareStatement("select id, bitmap from test.bitmap_test where id = 'company_bond_plates'").executeQuery();
        log.info("query success");
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
        connection.close();
    }
}
