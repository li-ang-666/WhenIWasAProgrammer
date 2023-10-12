package com.liang.hudi.job;

import com.liang.common.util.ApolloUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MultiStreamJob {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("rest.bind-port", "54321");
        configuration.setString("state.checkpoints.dir", "file:///Users/liang/Desktop/flink-checkpoints");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(1000 * 60, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String content = ApolloUtils.get("flink-sqls");
        for (String sql : content.split(";")) {
            if (sql.contains("CREATE TABLE")) {
                tEnv.executeSql(sql);
            }
        }
        StreamStatementSet statementSet = tEnv.createStatementSet();
        statementSet.addInsertSql("insert into dwd_ratio_path_company select * from ods_ratio_path_company");
        statementSet.addInsertSql("insert into dwd_enterprise select * from ods_enterprise");
        statementSet.execute();

    }
}
