package com.liang.hudi.job;

import com.liang.hudi.basic.TableEnvironmentFactory;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestJob {
    public static void main(String[] args) {
        StreamTableEnvironment tEnv = TableEnvironmentFactory.create();
        tEnv.getConfig().set("parallelism.default", "1");
        tEnv.executeSql("CREATE TABLE ods (\n" +
                "  id decimal(20,0),\n" +
                "  op_ts as CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)),\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://ee59dd05fc0f4bb9a2497c8d9146a53cin01.internal.cn-north-4.mysql.rds.myhuaweicloud.com:3306/company_base?useSSL=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false',\n" +
                "  'table-name' = 'company_index',\n" +
                "  'username' = 'jdhw_d_data_dml',\n" +
                "  'password' = '2s0^tFa4SLrp72',\n" +
                "  'scan.partition.column' = 'id',\n" +
                "  'scan.partition.lower-bound' = '1',\n" +
                "  'scan.partition.upper-bound' = '102400',\n" +
                "  'scan.partition.num' = '10'\n" +
                ")");
        tEnv.executeSql("select * from ods").print();
    }
}
