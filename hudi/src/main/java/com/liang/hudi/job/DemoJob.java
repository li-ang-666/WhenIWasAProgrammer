package com.liang.hudi.job;


import com.liang.common.util.ApolloUtils;
import com.liang.common.util.DateTimeUtils;
import com.liang.hudi.basic.TableEnvironmentFactory;
import com.liang.hudi.basic.TableFactory;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DemoJob {
    public static void main(String[] args) throws Exception {
        // create env
        StreamTableEnvironment tEnv = TableEnvironmentFactory.create();
        // exec sql
        tEnv.executeSql(TableFactory.fromFile("cdc.sql"));
        tEnv.executeSql(ApolloUtils.get("hudi_table").replaceAll("uuid", String.valueOf(DateTimeUtils.currentTimestamp())));
        tEnv.executeSql("insert into hudi_table select id,company_id,shareholder_id,shareholder_entity_type,shareholder_name_id,investment_ratio_total,is_controller,is_ultimate,is_big_shareholder,is_controlling_shareholder,equity_holding_path,create_time,update_time,is_deleted from cdc_table").print();
    }
}
