package com.liang.hudi.job;

import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.hudi.basic.TableEnvironmentFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.nio.charset.StandardCharsets;

@Slf4j
public class TestJob {
    public static void main(String[] args) {
        StreamTableEnvironment tEnv = TableEnvironmentFactory.create();
        tEnv.getConfig().set("pipeline.operator-chaining", "false");
        tEnv.getConfig().set("parallelism.default", "1");
        String sqls = ResourceUtil.readStr("sql/test.sql", StandardCharsets.UTF_8);
        for (String sql : sqls.split(";")) {
            if (StrUtil.isNotBlank(sql)) {
                log.info("execute sql: {}", sql);
                tEnv.executeSql(sql);
            }
        }
        tEnv.executeSql("select * from ods").print();
    }
}
