package com.liang.hudi.job;


import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class DemoPrintJob {
    public static void main(String[] args) throws Exception {
        // read file
        InputStream stream1 = DemoPrintJob.class.getClassLoader().getResourceAsStream("sqls/kafka.sql");
        String source = IOUtils.toString(stream1, StandardCharsets.UTF_8);
        InputStream stream2 = DemoPrintJob.class.getClassLoader().getResourceAsStream("sqls/hudi.sql");
        String sink = IOUtils.toString(stream2, StandardCharsets.UTF_8);

        // create env
        Configuration configuration = new Configuration();
        configuration.setString("rest.bind-port", "12345");
        configuration.setString("state.checkpoints.dir", "file:///Users/liang/Desktop/flink-checkpoints/");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointInterval(1000 * 30);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(source);
        tEnv.executeSql(sink);

        tEnv.executeSql("select * from hudi_table").print();
    }
}
