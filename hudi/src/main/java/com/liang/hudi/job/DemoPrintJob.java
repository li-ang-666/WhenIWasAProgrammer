package com.liang.hudi.job;


import com.liang.hudi.basic.TableEnvironmentFactory;
import com.liang.hudi.basic.TableFactory;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DemoPrintJob {
    public static void main(String[] args) throws Exception {
        // create env
        StreamTableEnvironment tEnv = TableEnvironmentFactory.create();
        // exec sql
        tEnv.executeSql(TableFactory.fromFile("read.sql"));
        tEnv.executeSql("select id,count(1) cnt from enterprise group by id").print();
    }
}
