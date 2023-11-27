package com.liang.hudi.job;


import com.liang.hudi.basic.TableFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DemoPrintJob {
    public static void main(String[] args) {
        // create env
        Configuration configuration = new Configuration();
        configuration.setString("state.checkpoints.dir", "file:///Users/liang/Desktop/flink-checkpoints");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(1000 * 10, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // exec sql
        tEnv.executeSql(TableFactory.fromFile("read.sql"));
        tEnv.executeSql("select * from ods").print();
    }
}
