package com.liang.hudi.job;


import com.liang.common.util.ApolloUtils;
import com.liang.hudi.basic.TableEnvironmentFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DemoJob {
    public static void main(String[] args) throws Exception {
        // create env
        StreamTableEnvironment tEnv = TableEnvironmentFactory.create();
        // exec sql
        for (String sql : ApolloUtils.get("flink-sqls").split(";")) {
            if (StringUtils.isNotBlank(sql)) {
                tEnv.executeSql(sql);
            }
        }
    }
}
