package com.liang.flink.test;

import com.liang.common.service.storage.FsWriter;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.HashMap;

@Slf4j
public class FlinkTest {
    public static void main(String[] args) throws Exception {
        FsWriter fsWriter = new FsWriter("obs://hadoop-obs/flink/pqt");
        HashMap<String, Object> columnMap = new HashMap<String, Object>() {{
            put("id", 1);
            put("name", "XM");
            put("age", new BigDecimal("20"));
        }};
        fsWriter.write(columnMap);
        fsWriter.flush();
        Thread.sleep(1000);
        fsWriter.write(columnMap);
        fsWriter.flush();
        Thread.sleep(1000);
        fsWriter.flush();
    }
}
