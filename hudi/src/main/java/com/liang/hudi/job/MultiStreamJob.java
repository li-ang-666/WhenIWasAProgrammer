package com.liang.hudi.job;

import com.liang.common.util.ApolloUtils;
import com.liang.hudi.basic.TableEnvironmentFactory;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MultiStreamJob {
    public static void main(String[] args) {
        StreamTableEnvironment tEnv = TableEnvironmentFactory.create();
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
