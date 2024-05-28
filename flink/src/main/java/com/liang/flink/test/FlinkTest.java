package com.liang.flink.test;

import com.liang.common.service.storage.FsParquetWriter;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.HashMap;

@Slf4j
public class FlinkTest {
    public static void main(String[] args) throws Exception {
        FsParquetWriter fsParquetWriter = new FsParquetWriter("obs://hadoop-obs/flink/pqt");
        HashMap<String, Object> columnMap = new HashMap<String, Object>() {{
            put("id", 1);
            put("name", "XM");
            put("age", new BigDecimal("20"));
        }};
        fsParquetWriter.write(columnMap);
        fsParquetWriter.flush();
        Thread.sleep(1000);
        fsParquetWriter.write(columnMap);
        fsParquetWriter.flush();
        Thread.sleep(1000);
        fsParquetWriter.flush();
    }
}
