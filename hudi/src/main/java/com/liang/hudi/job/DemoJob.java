package com.liang.hudi.job;


import org.apache.commons.io.IOUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class DemoJob {
    public static void main(String[] args) throws Exception {
        // read file
        InputStream stream = DemoJob.class.getClassLoader().getResourceAsStream("DemoJob/source.sql");
        String sql = IOUtils.toString(stream, StandardCharsets.UTF_8);

        // create env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(sql);

        tEnv.executeSql("select * from enterprise").print();
    }
}
