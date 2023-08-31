package com.liang.hudi.job;


import com.liang.hudi.basic.TableFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DemoJob {
    public static void main(String[] args) throws Exception {
        // create env
        Configuration configuration = new Configuration();
        configuration.setString("rest.bind-port", "54321");
        configuration.setString("state.checkpoints.dir", "file:///Users/liang/Desktop/flink-checkpoints/");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointInterval(1000 * 30);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // exec sql
        tEnv.executeSql(TableFactory.fromFile("cdc.sql"));
        tEnv.executeSql("select * from cdc_table").print();
        //tEnv.executeSql("insert into hudi_table select id,company_id,shareholder_id,shareholder_entity_type,shareholder_name_id,investment_ratio_total,is_controller,is_ultimate,is_big_shareholder,is_controlling_shareholder,equity_holding_path,create_time,update_time,is_deleted from ratio_path_company").print();
    }
}
