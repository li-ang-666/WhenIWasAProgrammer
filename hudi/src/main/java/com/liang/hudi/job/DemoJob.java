package com.liang.hudi.job;


import com.liang.hudi.basic.TableEnvironmentFactory;
import com.liang.hudi.basic.TableFactory;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DemoJob {
    public static void main(String[] args) throws Exception {
        // create env
        StreamTableEnvironment tEnv = TableEnvironmentFactory.create();
        // exec sql
        tEnv.executeSql(TableFactory.fromFile("cdc.sql"));
        tEnv.executeSql(TableFactory.fromFile("sink.sql"));
//        tEnv.executeSql(TableFactory.fromFile("hudi.sql"));
//        tEnv.executeSql("insert into hudi_table select id,company_id,shareholder_id,shareholder_entity_type,shareholder_name_id,investment_ratio_total,is_controller,is_ultimate,is_big_shareholder,is_controlling_shareholder,equity_holding_path,create_time,update_time,is_deleted from cdc_table").print();
        tEnv.executeSql("insert into flink_sql_sink select name,count(distinct id) cnt from cdc_table group by name").print();
    }
}
