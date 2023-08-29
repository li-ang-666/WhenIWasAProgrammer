package com.liang.hudi.job;


import org.apache.commons.io.IOUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class DemoJob {
    public static void main(String[] args) throws Exception {
        // read file
        InputStream stream1 = DemoJob.class.getClassLoader().getResourceAsStream("DemoJob/source.sql");
        String source = IOUtils.toString(stream1, StandardCharsets.UTF_8);
        InputStream stream2 = DemoJob.class.getClassLoader().getResourceAsStream("DemoJob/sink.sql");
        String sink = IOUtils.toString(stream2, StandardCharsets.UTF_8);

        // create env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointInterval(1000);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(source);
        tEnv.executeSql(sink);

        tEnv.executeSql("insert into hudi_table select id from ratio_path_company").print();
    }
}
