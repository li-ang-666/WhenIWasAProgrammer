package com.liang.hudi.job;

import com.liang.hudi.basic.TableEnvironmentFactory;
import com.liang.hudi.basic.TableFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.hudi.common.model.WriteOperationType.BULK_INSERT;

@Slf4j
public class BulkInsertJob {
    public static void main(String[] args) {
        // create env
        StreamTableEnvironment tEnv = TableEnvironmentFactory.create();
        // exec sql
        String sqls = TableFactory.fromTemplate(BULK_INSERT, args[1], args[2]);
        for (String sql : sqls.split(";")) {
            if (StringUtils.isNotBlank(sql)) {
                log.info("sql: {}", sql);
                tEnv.executeSql(sql);
            }
        }
    }
}